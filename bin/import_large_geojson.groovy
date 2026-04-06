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
    print "  Reading GeoJSON..."
    def pathObjects = PathIO.readObjects(geojsonFile.toPath())
    long tRead = System.currentTimeMillis()
    print "  Read ${pathObjects.size()} objects in ${(tRead - t0) / 1000.0}s"

    if (pathObjects.isEmpty()) {
        print "  WARNING: GeoJSON contained no objects, skipping"
        print "═".repeat(60)
        return
    }

    def hierarchy = getCurrentHierarchy()

    if (clearExisting) {
        int existingCount = hierarchy.getAllObjects(false).size()
        if (existingCount > 0) {
            hierarchy.clearAll()
            print "  Cleared ${existingCount} existing objects"
        }
    }

    hierarchy.addObjects(pathObjects)
    hierarchy.resolveHierarchy()

    long tDone = System.currentTimeMillis()
    print "  OK: Imported ${pathObjects.size()} objects in ${(tDone - t0) / 1000.0}s"

} catch (Exception e) {
    print "ERROR processing '${imageName}': ${e.getMessage()}"
    e.printStackTrace()
}

print "═".repeat(60)
