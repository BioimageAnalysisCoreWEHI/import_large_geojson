import qupath.lib.objects.PathObject
import qupath.lib.io.GsonTools
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.google.gson.JsonElement
import java.util.concurrent.Executors
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.zip.GZIPInputStream
import qupath.lib.objects.PathDetectionObject
import qupath.lib.objects.PathAnnotationObject
import org.slf4j.LoggerFactory

// ============================================================
// LOGGING
// ============================================================
// Route all script output through slf4j/logback rather than the scripting
// `print` builtin. QuPath's per-image batch runner (0.7+) captures a script's
// stdout and only emits it when the script returns normally — so any output
// produced before this script's `System.exit(0)` (used below to skip the
// remaining project images) is silently discarded. logback's ConsoleAppender
// holds the original stdout and bypasses that capture, so log lines survive the
// exit and reach the per-image .log file that is this pipeline's only artifact.
def log = LoggerFactory.getLogger('import_large_geojson')

// ============================================================
// CONFIGURATION (from environment variables set by Nextflow)
// ============================================================

def envVars = System.getenv()

def geojsonDir    = envVars.getOrDefault('GEOJSON_DIR', '')
def clearRaw      = envVars.getOrDefault('CLEAR_EXISTING', 'true').trim().toLowerCase()
def clearExisting = (clearRaw == '1' || clearRaw == 'true' || clearRaw == 'yes' || clearRaw == 'y')
def filePattern   = envVars.getOrDefault('FILE_PATTERN', '{stem}.geojson')

// Skip resolveHierarchy if all objects are flat detections (no parent-child nesting).
// resolveHierarchy() is O(n^2) and is the single biggest bottleneck at scale.
// Set RESOLVE_HIERARCHY=false in Nextflow env for a major speedup.
def resolveHierarchyRaw = envVars.getOrDefault('RESOLVE_HIERARCHY', 'true').trim().toLowerCase()
def doResolveHierarchy  = (resolveHierarchyRaw == '1' || resolveHierarchyRaw == 'true' || resolveHierarchyRaw == 'yes')

if (!geojsonDir) {
    log.error "GEOJSON_DIR environment variable is not set."
    return
}

def geojsonDirFile = new File(geojsonDir)
if (!geojsonDirFile.exists() || !geojsonDirFile.isDirectory()) {
    log.error "GeoJSON directory does not exist or is not a directory: ${geojsonDir}"
    return
}

// ============================================================
// MAIN — QuPath runs this script once per image in batch mode
// ============================================================

def imageName = getProjectEntry()?.getImageName() ?: '(unknown image)'

// Strip extension to get stem (handles .ome.tif, .ome.tiff, .tiff, etc.)
def stem = imageName
    .replaceAll(/(?i)\.ome\.tiff?$/, "")
    .replaceAll(/\.[^.]+$/, "")

// Per-image parallelism: if IMAGE_STEM is set, only process the matching image.
// This allows Nextflow to launch one QuPath process per image concurrently.
// Match by checking imageName == targetStem (no extension) or imageName starts with targetStem + '.'
// This handles dots in image stems (e.g. "01041-2.1_Scan1") that confuse naive extension-stripping.
def targetStem = envVars.getOrDefault('IMAGE_STEM', '')
def matchedTarget = false
if (targetStem) {
    def imageMatchesStem = (imageName == targetStem) || imageName.startsWith(targetStem + '.')
    if (!imageMatchesStem) {
        log.info "  Skipping '${imageName}' (does not match target stem '${targetStem}')"
        return
    }
    matchedTarget = true
}

// When targetStem is set, use it directly for the GeoJSON lookup — it is derived from
// the actual GeoJSON filename and is more reliable than the stem extracted from the
// QuPath image name (which can be wrong when the name contains dots).
def effectiveStem = targetStem ?: stem

// Build expected GeoJSON filename from pattern
def geojsonFileName = filePattern.replace('{stem}', effectiveStem)
def geojsonFile = new File(geojsonDirFile, geojsonFileName)

// Also check for gzipped variant
if (!geojsonFile.exists() && !geojsonFileName.endsWith('.gz')) {
    def gzFile = new File(geojsonDirFile, geojsonFileName + '.gz')
    if (gzFile.exists()) {
        geojsonFile = gzFile
        geojsonFileName = geojsonFileName + '.gz'
    }
}

def isGzipped = geojsonFile.name.endsWith('.gz')

log.info "═".repeat(60)
log.info "Import Large GeoJSON into QuPath"
log.info "═".repeat(60)
log.info "  Image             : ${imageName}"
log.info "  Stem              : ${stem}"
log.info "  GeoJSON directory : ${geojsonDir}"
log.info "  Looking for       : ${geojsonFileName}${isGzipped ? '' : ' (or .gz)'}"
log.info "  Clear existing    : ${clearExisting}"
log.info "  Resolve hierarchy : ${doResolveHierarchy}"

if (!geojsonFile.exists()) {
    log.warn "No GeoJSON found for '${imageName}' (looked for ${filePattern.replace('{stem}', stem)}{,.gz})"
    log.warn "  Available files in directory:"
    geojsonDirFile.listFiles()?.findAll { it.name.endsWith('.geojson') || it.name.endsWith('.geojson.gz') }?.take(20)?.each {
        log.warn "    ${it.name}"
    }
    log.info "═".repeat(60)
    return
}

def fileSizeMB = geojsonFile.length() / (1024.0 * 1024.0)
log.info "  File size         : ${String.format('%.1f', fileSizeMB)} MB"

try {
    long t0 = System.currentTimeMillis()

    // ── Stream-parse GeoJSON one feature at a time ──────────────
    // PathIO.readObjects() loads the ENTIRE JSON tree via Gson,
    // which for 100+ GB files requires 3-5x file size in heap.
    // Instead, we use Gson's streaming JsonReader to parse one
    // feature at a time, then convert to PathObject in parallel
    // across a thread pool for maximum throughput.
    // ────────────────────────────────────────────────────────────
    def nThreads = Math.max(2, Runtime.getRuntime().availableProcessors())
    def BATCH_SIZE = 5000
    log.info "  [1/5] Stream-parsing GeoJSON file (${String.format('%.1f', fileSizeMB)} MB) with ${nThreads} threads, batch size ${BATCH_SIZE}..."

    def gson = GsonTools.getInstance()
    def jsonElementAdapter = gson.getAdapter(JsonElement.class)
    def pathObjects = Collections.synchronizedList(new ArrayList<PathObject>(500_000))
    int featureCount = 0
    int errorCount = 0

    def executor = Executors.newFixedThreadPool(nThreads)
    def pendingFutures = new ArrayList<Future<List>>()

    // Submit a batch of JsonElements for parallel conversion to PathObjects
    def submitBatch = { List<JsonElement> batch ->
        def localBatch = new ArrayList<JsonElement>(batch)
        pendingFutures.add(executor.submit({
            def results = new ArrayList<PathObject>(localBatch.size())
            int localErrors = 0
            for (elem in localBatch) {
                try {
                    def obj = gson.fromJson(elem, PathObject.class)
                    if (obj != null) results.add(obj)
                } catch (Exception fe) {
                    localErrors++
                }
            }
            return [results, localErrors]
        } as Callable<List>))
    }

    // Drain completed futures to free memory
    def drainFutures = {
        for (future in pendingFutures) {
            def result = future.get()
            List<PathObject> objs = result[0]
            int errs = result[1]
            pathObjects.addAll(objs)
            errorCount += errs
        }
        pendingFutures.clear()
    }

    def currentBatch = new ArrayList<JsonElement>(BATCH_SIZE)

    def fis = new FileInputStream(geojsonFile)
    def bis = new BufferedInputStream(fis, 64 * 1024 * 1024)  // 64 MB read buffer
    def rawStream = isGzipped ? new GZIPInputStream(bis, 64 * 1024 * 1024) : bis
    def isr = new InputStreamReader(rawStream, 'UTF-8')
    def reader = new JsonReader(isr)
    reader.setLenient(true)

    // Closure to process one feature element from the stream
    def processElement = { ->
        def element = jsonElementAdapter.read(reader)
        currentBatch.add(element)
        featureCount++

        if (currentBatch.size() >= BATCH_SIZE) {
            submitBatch(currentBatch)
            currentBatch = new ArrayList<JsonElement>(BATCH_SIZE)

            // Drain futures periodically to avoid unbounded memory growth
            // (drain every nThreads*2 batches so the pool stays fed)
            if (pendingFutures.size() >= nThreads * 2) {
                drainFutures()
            }
        }

        if (featureCount % 100_000 == 0) {
            def rt = Runtime.getRuntime()
            def usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024L * 1024L)
            def maxMB  = rt.maxMemory() / (1024L * 1024L)
            def elapsed = (System.currentTimeMillis() - t0) / 1000.0
            def rate = featureCount / elapsed
            log.info "    ... parsed ${featureCount} features (${pathObjects.size()} converted, memory: ${usedMB}/${maxMB} MB, ${String.format('%.0f', rate)} feat/s)"
        }
    }

    try {
        def firstToken = reader.peek()

        if (firstToken == JsonToken.BEGIN_OBJECT) {
            // Standard FeatureCollection: { "type": "FeatureCollection", "features": [...] }
            reader.beginObject()
            boolean foundFeatures = false
            while (reader.hasNext()) {
                def key = reader.nextName()
                if (key == "features") {
                    foundFeatures = true
                    reader.beginArray()
                    while (reader.hasNext()) {
                        processElement()
                    }
                    reader.endArray()
                } else {
                    reader.skipValue()
                }
            }
            reader.endObject()
            if (!foundFeatures) {
                log.error "  JSON object had no 'features' key — is this a valid GeoJSON FeatureCollection?"
                log.info "═".repeat(60)
                executor.shutdownNow()
                return
            }

        } else if (firstToken == JsonToken.BEGIN_ARRAY) {
            // Bare array of features: [ { "type": "Feature", ... }, ... ]
            reader.beginArray()
            while (reader.hasNext()) {
                processElement()
            }
            reader.endArray()

        } else {
            log.error "  Unexpected JSON structure (expected object or array, got ${firstToken})"
            log.info "═".repeat(60)
            executor.shutdownNow()
            return
        }
    } finally {
        reader.close()
    }

    // Submit any remaining features
    if (!currentBatch.isEmpty()) {
        submitBatch(currentBatch)
    }

    // Drain all remaining futures
    drainFutures()
    executor.shutdown()

    long tRead = System.currentTimeMillis()
    def rt = Runtime.getRuntime()
    def usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024L * 1024L)
    def maxMB  = rt.maxMemory() / (1024L * 1024L)
    log.info "  [1/5] Read complete: ${pathObjects.size()} objects from ${featureCount} features in ${(tRead - t0) / 1000.0}s (memory: ${usedMB}/${maxMB} MB)"
    if (errorCount > 0) {
        log.warn "  ${errorCount} features failed to parse"
    }

    if (pathObjects.isEmpty()) {
        log.warn "  GeoJSON contained no valid objects, skipping"
        log.info "═".repeat(60)
        return
    }

    // Count object types for reporting
    def typeCounts = pathObjects.groupBy { it.getClass().getSimpleName() }.collectEntries { k, v -> [k, v.size()] }
    log.info "  Object types: ${typeCounts}"

    // Separate annotations from detections — adding annotations first lets
    // QuPath build its spatial index once, then bulk-insert detections.
    def annotations = pathObjects.findAll { it instanceof PathAnnotationObject }
    def detections  = pathObjects.findAll { it instanceof PathDetectionObject }
    def others      = pathObjects.findAll { !(it instanceof PathAnnotationObject) && !(it instanceof PathDetectionObject) }
    log.info "  Annotations: ${annotations.size()}, Detections: ${detections.size()}, Other: ${others.size()}"

    log.info "  [2/5] Getting current hierarchy..."
    def hierarchy = getCurrentHierarchy()
    long tHierarchy = System.currentTimeMillis()
    log.info "  [2/5] Hierarchy loaded in ${(tHierarchy - tRead) / 1000.0}s"

    if (clearExisting) {
        int existingCount = hierarchy.getAllObjects(false).size()
        if (existingCount > 0) {
            log.info "  [3/5] Clearing ${existingCount} existing objects..."
            hierarchy.clearAll()
            long tClear = System.currentTimeMillis()
            log.info "  [3/5] Cleared in ${(tClear - tHierarchy) / 1000.0}s"
        } else {
            log.info "  [3/5] No existing objects to clear"
        }
    } else {
        int existingCount = hierarchy.getAllObjects(false).size()
        log.info "  [3/5] Keeping ${existingCount} existing objects (clear_existing=false)"
    }

    // Add in order: annotations -> detections -> other
    // This avoids repeated spatial index rebuilds inside QuPath.
    log.info "  [4/5] Adding objects (annotations first, then detections)..."
    long tAddStart = System.currentTimeMillis()

    if (!annotations.isEmpty()) {
        annotations.each { it.setLocked(true) }
        hierarchy.addObjects(annotations)
        log.info "    Added ${annotations.size()} annotations (locked) in ${(System.currentTimeMillis() - tAddStart) / 1000.0}s"
    }

    long tDetStart = System.currentTimeMillis()
    if (!detections.isEmpty()) {
        // For very large counts, add in chunks to avoid a single enormous hierarchy update
        if (detections.size() > 200_000) {
            def chunkSize = 100_000
            def chunks = detections.collate(chunkSize)
            chunks.eachWithIndex { chunk, idx ->
                hierarchy.addObjects(chunk)
                log.info "    Detection chunk ${idx + 1}/${chunks.size()} added (${chunk.size()} objects)"
            }
        } else {
            hierarchy.addObjects(detections)
        }
        log.info "    Added ${detections.size()} detections in ${(System.currentTimeMillis() - tDetStart) / 1000.0}s"
    }

    if (!others.isEmpty()) {
        hierarchy.addObjects(others)
        log.info "    Added ${others.size()} other objects"
    }

    long tAdd = System.currentTimeMillis()
    log.info "  [4/5] All objects added in ${(tAdd - tAddStart) / 1000.0}s"

    // resolveHierarchy is O(n^2) and VERY expensive at scale.
    // Skip it if objects are all flat detections (no nesting needed).
    // Set RESOLVE_HIERARCHY=false in Nextflow env to bypass.
    if (doResolveHierarchy) {
        log.info "  [4/5] Resolving hierarchy (set RESOLVE_HIERARCHY=false to skip)..."
        hierarchy.resolveHierarchy()
        long tResolve = System.currentTimeMillis()
        log.info "  [4/5] Resolved in ${(tResolve - tAdd) / 1000.0}s"
    } else {
        log.info "  [4/5] Skipping resolveHierarchy (RESOLVE_HIERARCHY=false)"
    }

    log.info "  [5/5] Firing hierarchy update and saving..."
    fireHierarchyUpdate()
    def entry = getProjectEntry()
    entry.saveImageData(getCurrentImageData())
    long tDone = System.currentTimeMillis()
    log.info "  [5/5] Saved in ${(tDone - tAdd) / 1000.0}s"

    def totalObjects = hierarchy.getAllObjects(false).size()
    log.info "  OK: Imported ${pathObjects.size()} objects in ${(tDone - t0) / 1000.0}s (total in hierarchy: ${totalObjects})"

} catch (Exception e) {
    log.error("Error processing '${imageName}': ${e.getMessage()}", e)
}

log.info "═".repeat(60)

// Per-image mode: target was located and processed (success or failure).
// Skip the remaining images in the project — each one costs ~20s of QuPath
// image initialization before our script even gets a chance to `return`.
// Safe because Nextflow launches one QuPath process per IMAGE_STEM and the
// project is guaranteed to contain no duplicate stems.
//
// NOTE: all output above goes through slf4j/logback (not `print`) precisely so
// it survives this System.exit(0) — see the LOGGING note at the top of the file.
if (matchedTarget) {
    log.info "Per-image mode: target processed, exiting JVM to skip remaining images."
    System.exit(0)
}
