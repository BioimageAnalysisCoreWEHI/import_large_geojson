import qupath.lib.objects.PathObject
import qupath.lib.io.GsonTools
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.google.gson.JsonElement

// ============================================================
// CONFIGURATION (from environment variables set by Nextflow)
// ============================================================

def envVars = System.getenv()

def geojsonDir    = envVars.getOrDefault('GEOJSON_DIR', '')
def clearRaw      = envVars.getOrDefault('CLEAR_EXISTING', 'true').trim().toLowerCase()
def clearExisting = (clearRaw == '1' || clearRaw == 'true' || clearRaw == 'yes' || clearRaw == 'y')
def filePattern   = envVars.getOrDefault('FILE_PATTERN', '{stem}.geojson')

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

print "═".repeat(60)
print "Import Large GeoJSON into QuPath"
print "═".repeat(60)
print "  Image             : ${imageName}"
print "  Stem              : ${stem}"
print "  GeoJSON directory : ${geojsonDir}"
print "  Looking for       : ${geojsonFileName}"
print "  Clear existing    : ${clearExisting}"

if (!geojsonFile.exists()) {
    print "WARNING: No GeoJSON found for '${imageName}' (looked for ${geojsonFileName})"
    print "  Available files in directory:"
    geojsonDirFile.listFiles()?.findAll { it.name.endsWith('.geojson') }?.take(20)?.each {
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
    // feature at a time, convert it to a PathObject, then discard
    // the intermediate JsonElement immediately.
    // ────────────────────────────────────────────────────────────
    print "  [1/4] Stream-parsing GeoJSON file (${String.format('%.1f', fileSizeMB)} MB)..."

    def gson = GsonTools.getInstance()
    def jsonElementAdapter = gson.getAdapter(JsonElement.class)
    def pathObjects = []
    int featureCount = 0
    int errorCount = 0

    def fis = new FileInputStream(geojsonFile)
    def bis = new BufferedInputStream(fis, 8 * 1024 * 1024)  // 8 MB read buffer
    def isr = new InputStreamReader(bis, 'UTF-8')
    def reader = new JsonReader(isr)
    reader.setLenient(true)

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
                        def element = jsonElementAdapter.read(reader)
                        try {
                            def obj = gson.fromJson(element, PathObject.class)
                            if (obj != null) pathObjects.add(obj)
                        } catch (Exception fe) {
                            errorCount++
                            if (errorCount <= 5) {
                                print "    WARNING: Failed to parse feature #${featureCount}: ${fe.getMessage()}"
                            }
                        }
                        featureCount++
                        if (featureCount % 50000 == 0) {
                            def rt = Runtime.getRuntime()
                            def usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024L * 1024L)
                            def maxMB  = rt.maxMemory() / (1024L * 1024L)
                            print "    ... parsed ${featureCount} features (${pathObjects.size()} objects, memory: ${usedMB}/${maxMB} MB)"
                        }
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
                return
            }

        } else if (firstToken == JsonToken.BEGIN_ARRAY) {
            // Bare array of features: [ { "type": "Feature", ... }, ... ]
            reader.beginArray()
            while (reader.hasNext()) {
                def element = jsonElementAdapter.read(reader)
                try {
                    def obj = gson.fromJson(element, PathObject.class)
                    if (obj != null) pathObjects.add(obj)
                } catch (Exception fe) {
                    errorCount++
                    if (errorCount <= 5) {
                        print "    WARNING: Failed to parse feature #${featureCount}: ${fe.getMessage()}"
                    }
                }
                featureCount++
                if (featureCount % 50000 == 0) {
                    def rt = Runtime.getRuntime()
                    def usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024L * 1024L)
                    def maxMB  = rt.maxMemory() / (1024L * 1024L)
                    print "    ... parsed ${featureCount} features (${pathObjects.size()} objects, memory: ${usedMB}/${maxMB} MB)"
                }
            }
            reader.endArray()

        } else {
            print "  ERROR: Unexpected JSON structure (expected object or array, got ${firstToken})"
            print "═".repeat(60)
            return
        }
    } finally {
        reader.close()
    }

    long tRead = System.currentTimeMillis()
    def rt = Runtime.getRuntime()
    def usedMB = (rt.totalMemory() - rt.freeMemory()) / (1024L * 1024L)
    def maxMB  = rt.maxMemory() / (1024L * 1024L)
    print "  [1/4] Read complete: ${pathObjects.size()} objects from ${featureCount} features in ${(tRead - t0) / 1000.0}s (memory: ${usedMB}/${maxMB} MB)"
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

    print "  [2/4] Getting current hierarchy..."
    def hierarchy = getCurrentHierarchy()
    long tHierarchy = System.currentTimeMillis()
    print "  [2/4] Hierarchy loaded in ${(tHierarchy - tRead) / 1000.0}s"

    if (clearExisting) {
        int existingCount = hierarchy.getAllObjects(false).size()
        if (existingCount > 0) {
            print "  [3/4] Clearing ${existingCount} existing objects..."
            hierarchy.clearAll()
            long tClear = System.currentTimeMillis()
            print "  [3/4] Cleared in ${(tClear - tHierarchy) / 1000.0}s"
        } else {
            print "  [3/4] No existing objects to clear"
        }
    } else {
        int existingCount = hierarchy.getAllObjects(false).size()
        print "  [3/4] Keeping ${existingCount} existing objects (clear_existing=false)"
    }

    print "  [4/4] Adding ${pathObjects.size()} objects to hierarchy..."
    long tAddStart = System.currentTimeMillis()
    hierarchy.addObjects(pathObjects)
    long tAdd = System.currentTimeMillis()
    print "  [4/4] Added objects in ${(tAdd - tAddStart) / 1000.0}s"

    print "  [4/4] Resolving hierarchy..."
    hierarchy.resolveHierarchy()
    long tResolve = System.currentTimeMillis()
    print "  [4/4] Resolved in ${(tResolve - tAdd) / 1000.0}s"

    print "  [5/5] Firing hierarchy update..."
    fireHierarchyUpdate()
    long tDone = System.currentTimeMillis()
    print "  [5/5] Update fired in ${(tDone - tResolve) / 1000.0}s"

    def totalObjects = hierarchy.getAllObjects(false).size()
    print "  OK: Imported ${pathObjects.size()} objects in ${(tDone - t0) / 1000.0}s (total in hierarchy: ${totalObjects})"

} catch (Exception e) {
    print "ERROR processing '${imageName}': ${e.getMessage()}"
    e.printStackTrace()
}

print "═".repeat(60)
