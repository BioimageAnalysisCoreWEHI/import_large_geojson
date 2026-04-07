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
    print "ERROR: GEOJSON_DIR environment variable is not set."
    return
}

def geojsonDirFile = new File(geojsonDir)
if (!geojsonDirFile.exists() || !geojsonDirFile.isDirectory()) {
    print "ERROR: GeoJSON directory does not exist or is not a directory: ${geojsonDir}"
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

// Build expected GeoJSON filename from pattern
def geojsonFileName = filePattern.replace('{stem}', stem)
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

print "═".repeat(60)
print "Import Large GeoJSON into QuPath"
print "═".repeat(60)
print "  Image             : ${imageName}"
print "  Stem              : ${stem}"
print "  GeoJSON directory : ${geojsonDir}"
print "  Looking for       : ${geojsonFileName}${isGzipped ? '' : ' (or .gz)'}"
print "  Clear existing    : ${clearExisting}"
print "  Resolve hierarchy : ${doResolveHierarchy}"

if (!geojsonFile.exists()) {
    print "WARNING: No GeoJSON found for '${imageName}' (looked for ${filePattern.replace('{stem}', stem)}{,.gz})"
    print "  Available files in directory:"
    geojsonDirFile.listFiles()?.findAll { it.name.endsWith('.geojson') || it.name.endsWith('.geojson.gz') }?.take(20)?.each {
        print "    ${it.name}"
    }
    print "═".repeat(60)
    return
}

def fileSizeMB = geojsonFile.length() / (1024.0 * 1024.0)
print "  File size         : ${String.format('%.1f', fileSizeMB)} MB"

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
    print "  [1/5] Stream-parsing GeoJSON file (${String.format('%.1f', fileSizeMB)} MB) with ${nThreads} threads, batch size ${BATCH_SIZE}..."

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
            print "    ... parsed ${featureCount} features (${pathObjects.size()} converted, memory: ${usedMB}/${maxMB} MB, ${String.format('%.0f', rate)} feat/s)"
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
                print "  ERROR: JSON object had no 'features' key — is this a valid GeoJSON FeatureCollection?"
                print "═".repeat(60)
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
            print "  ERROR: Unexpected JSON structure (expected object or array, got ${firstToken})"
            print "═".repeat(60)
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
    print "  [1/5] Read complete: ${pathObjects.size()} objects from ${featureCount} features in ${(tRead - t0) / 1000.0}s (memory: ${usedMB}/${maxMB} MB)"
    if (errorCount > 0) {
        print "  WARNING: ${errorCount} features failed to parse"
    }

    if (pathObjects.isEmpty()) {
        print "  WARNING: GeoJSON contained no valid objects, skipping"
        print "═".repeat(60)
        return
    }

    // Count object types for reporting
    def typeCounts = pathObjects.groupBy { it.getClass().getSimpleName() }.collectEntries { k, v -> [k, v.size()] }
    print "  Object types: ${typeCounts}"

    // Separate annotations from detections — adding annotations first lets
    // QuPath build its spatial index once, then bulk-insert detections.
    def annotations = pathObjects.findAll { it instanceof PathAnnotationObject }
    def detections  = pathObjects.findAll { it instanceof PathDetectionObject }
    def others      = pathObjects.findAll { !(it instanceof PathAnnotationObject) && !(it instanceof PathDetectionObject) }
    print "  Annotations: ${annotations.size()}, Detections: ${detections.size()}, Other: ${others.size()}"

    print "  [2/5] Getting current hierarchy..."
    def hierarchy = getCurrentHierarchy()
    long tHierarchy = System.currentTimeMillis()
    print "  [2/5] Hierarchy loaded in ${(tHierarchy - tRead) / 1000.0}s"

    if (clearExisting) {
        int existingCount = hierarchy.getAllObjects(false).size()
        if (existingCount > 0) {
            print "  [3/5] Clearing ${existingCount} existing objects..."
            hierarchy.clearAll()
            long tClear = System.currentTimeMillis()
            print "  [3/5] Cleared in ${(tClear - tHierarchy) / 1000.0}s"
        } else {
            print "  [3/5] No existing objects to clear"
        }
    } else {
        int existingCount = hierarchy.getAllObjects(false).size()
        print "  [3/5] Keeping ${existingCount} existing objects (clear_existing=false)"
    }

    // Add in order: annotations -> detections -> other
    // This avoids repeated spatial index rebuilds inside QuPath.
    print "  [4/5] Adding objects (annotations first, then detections)..."
    long tAddStart = System.currentTimeMillis()

    if (!annotations.isEmpty()) {
        hierarchy.addObjects(annotations)
        print "    Added ${annotations.size()} annotations in ${(System.currentTimeMillis() - tAddStart) / 1000.0}s"
    }

    long tDetStart = System.currentTimeMillis()
    if (!detections.isEmpty()) {
        // For very large counts, add in chunks to avoid a single enormous hierarchy update
        if (detections.size() > 200_000) {
            def chunkSize = 100_000
            def chunks = detections.collate(chunkSize)
            chunks.eachWithIndex { chunk, idx ->
                hierarchy.addObjects(chunk)
                print "    Detection chunk ${idx + 1}/${chunks.size()} added (${chunk.size()} objects)"
            }
        } else {
            hierarchy.addObjects(detections)
        }
        print "    Added ${detections.size()} detections in ${(System.currentTimeMillis() - tDetStart) / 1000.0}s"
    }

    if (!others.isEmpty()) {
        hierarchy.addObjects(others)
        print "    Added ${others.size()} other objects"
    }

    long tAdd = System.currentTimeMillis()
    print "  [4/5] All objects added in ${(tAdd - tAddStart) / 1000.0}s"

    // resolveHierarchy is O(n^2) and VERY expensive at scale.
    // Skip it if objects are all flat detections (no nesting needed).
    // Set RESOLVE_HIERARCHY=false in Nextflow env to bypass.
    if (doResolveHierarchy) {
        print "  [4/5] Resolving hierarchy (set RESOLVE_HIERARCHY=false to skip)..."
        hierarchy.resolveHierarchy()
        long tResolve = System.currentTimeMillis()
        print "  [4/5] Resolved in ${(tResolve - tAdd) / 1000.0}s"
    } else {
        print "  [4/5] Skipping resolveHierarchy (RESOLVE_HIERARCHY=false)"
    }

    print "  [5/5] Firing hierarchy update and saving..."
    fireHierarchyUpdate()
    def entry = getProjectEntry()
    entry.saveImageData(getCurrentImageData())
    long tDone = System.currentTimeMillis()
    print "  [5/5] Saved in ${(tDone - tAdd) / 1000.0}s"

    def totalObjects = hierarchy.getAllObjects(false).size()
    print "  OK: Imported ${pathObjects.size()} objects in ${(tDone - t0) / 1000.0}s (total in hierarchy: ${totalObjects})"

} catch (Exception e) {
    print "ERROR processing '${imageName}': ${e.getMessage()}"
    e.printStackTrace()
}

print "═".repeat(60)
