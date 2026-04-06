import qupath.lib.objects.PathObject

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

    // Read GeoJSON objects using QuPath's built-in reader
    print "  [1/4] Reading GeoJSON file (${String.format('%.1f', fileSizeMB)} MB)..."
    def pathObjects = PathIO.readObjects(geojsonFile.toPath())
    long tRead = System.currentTimeMillis()
    print "  [1/4] Read complete: ${pathObjects.size()} objects in ${(tRead - t0) / 1000.0}s"

    if (pathObjects.isEmpty()) {
        print "  WARNING: GeoJSON contained no objects, skipping"
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
    long tDone = System.currentTimeMillis()
    print "  [4/4] Resolved in ${(tDone - tAdd) / 1000.0}s"

    def totalObjects = hierarchy.getAllObjects(false).size()
    print "  OK: Imported ${pathObjects.size()} objects in ${(tDone - t0) / 1000.0}s (total in hierarchy: ${totalObjects})"

} catch (Exception e) {
    print "ERROR processing '${imageName}': ${e.getMessage()}"
    e.printStackTrace()
}

print "═".repeat(60)
