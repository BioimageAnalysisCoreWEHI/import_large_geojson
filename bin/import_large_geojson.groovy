import qupath.lib.objects.PathObject

// ============================================================
// CONFIGURATION (from environment variables set by Nextflow)
// ============================================================

def env = System.getenv()

def geojsonDir   = env.getOrDefault('GEOJSON_DIR', '')
def clearExisting = env.getOrDefault('CLEAR_EXISTING', 'true')
    .trim().toLowerCase() in ['1', 'true', 'yes', 'y']
def filePattern  = env.getOrDefault('FILE_PATTERN', '{stem}.geojson')

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
// MAIN
// ============================================================

def project = getProject()
if (project == null) {
    print "ERROR: No project is open."
    return
}

print "═".repeat(60)
print "Import Large GeoJSON into QuPath"
print "═".repeat(60)
print "  GeoJSON directory : ${geojsonDir}"
print "  Clear existing    : ${clearExisting}"
print "  File pattern      : ${filePattern}"
print "  Images in project : ${project.getImageList().size()}"
print ""

int successCount = 0
int failCount = 0
int skippedCount = 0
List<String> unmatched = []

for (def entry in project.getImageList()) {
    def imageName = entry.getImageName()
    // Strip extension to get stem (handles .ome.tif, .ome.tiff, .tiff, etc.)
    def stem = imageName
        .replaceAll(/\.ome\.tiff?$/i, "")
        .replaceAll(/\.[^.]+$/, "")

    // Build expected GeoJSON filename from pattern
    def geojsonFileName = filePattern.replace('{stem}', stem)
    def geojsonFile = new File(geojsonDirFile, geojsonFileName)

    if (!geojsonFile.exists()) {
        print "WARNING: No GeoJSON found for '${imageName}' (looked for ${geojsonFileName})"
        unmatched << imageName
        skippedCount++
        continue
    }

    def fileSizeMB = geojsonFile.length() / (1024.0 * 1024.0)
    print "Processing '${imageName}' <- ${geojsonFileName} (${String.format('%.1f', fileSizeMB)} MB)"

    try {
        long t0 = System.currentTimeMillis()

        // Read GeoJSON objects using QuPath's built-in reader
        def pathObjects = PathIO.readObjects(geojsonFile.toPath())
        long tRead = System.currentTimeMillis()
        print "  Read ${pathObjects.size()} objects in ${(tRead - t0) / 1000.0}s"

        if (pathObjects.isEmpty()) {
            print "  WARNING: GeoJSON contained no objects, skipping"
            skippedCount++
            continue
        }

        def imageData = entry.readImageData()
        def hierarchy = imageData.getHierarchy()

        if (clearExisting) {
            int existingCount = hierarchy.getAllObjects(false).size()
            if (existingCount > 0) {
                hierarchy.clearAll()
                print "  Cleared ${existingCount} existing objects"
            }
        }

        hierarchy.addObjects(pathObjects)
        hierarchy.resolveHierarchy()

        entry.saveImageData(imageData)

        long tDone = System.currentTimeMillis()
        print "  OK: Imported ${pathObjects.size()} objects in ${(tDone - t0) / 1000.0}s"
        successCount++

    } catch (Exception e) {
        print "ERROR processing '${imageName}': ${e.getMessage()}"
        e.printStackTrace()
        failCount++
    }
}

print ""
print "═".repeat(60)
print "Done. ${successCount} succeeded, ${failCount} failed, ${skippedCount} skipped."
if (unmatched) {
    print "Unmatched images (${unmatched.size()}): ${unmatched.join(', ')}"
}
print "═".repeat(60)
