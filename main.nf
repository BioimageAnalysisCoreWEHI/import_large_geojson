nextflow.enable.dsl = 2

params.project = null
params.qupath_bin = "/stornext/System/data/software/rhel/9/base/tools/QuPath/0.6.0/bin/QuPath"
params.script = "${projectDir}/bin/import_large_geojson.groovy"
params.geojson_dir = null
params.clear_existing = true
params.file_pattern = "{stem}.geojson"
params.resolve_hierarchy = true
params.outdir = "results"
params.publish_dir_mode = "copy"
params.validate_params = true

process IMPORT_LARGE_GEOJSON {
    tag "${image_stem}"
    label 'process_heavy'

    publishDir "${params.outdir}", mode: params.publish_dir_mode

    input:
    tuple val(project_path), val(qupath_bin), val(script_path), val(geojson_dir), val(clear_existing), val(file_pattern), val(resolve_hierarchy), val(image_stem)

    output:
    path "*.log"

    script:
    def safeTag = image_stem.replaceAll('[^a-zA-Z0-9_.-]', '_')
    """
    set -euo pipefail

    if [[ ! -f "${project_path}" ]]; then
      echo "ERROR: QuPath project not found: ${project_path}" >&2
      exit 1
    fi

    if [[ ! -x "${qupath_bin}" ]]; then
      echo "ERROR: QuPath binary is not executable: ${qupath_bin}" >&2
      exit 1
    fi

    if [[ ! -f "${script_path}" ]]; then
      echo "ERROR: Groovy script not found: ${script_path}" >&2
      exit 1
    fi

    if [[ ! -d "${geojson_dir}" ]]; then
      echo "ERROR: GeoJSON directory not found: ${geojson_dir}" >&2
      exit 1
    fi

    export GEOJSON_DIR="${geojson_dir}"
    export CLEAR_EXISTING="${clear_existing}"
    export FILE_PATTERN="${file_pattern}"
    export RESOLVE_HIERARCHY="${resolve_hierarchy}"
    export IMAGE_STEM="${image_stem}"

    "${qupath_bin}" script "${script_path}" --project "${project_path}" \
      2>&1 | tee "qupath_geojson_import_${safeTag}.log"
    """
}

workflow {
    if (!params.project) {
        error "Missing required parameter: --project"
    }
    if (!params.geojson_dir) {
        error "Missing required parameter: --geojson_dir"
    }

    def projectFile = file(params.project)
    if (!projectFile.exists()) {
        error "Project file does not exist: ${params.project}"
    }

    def qupathExe = file(params.qupath_bin)
    if (!qupathExe.exists()) {
        error "QuPath binary does not exist: ${params.qupath_bin}"
    }

    def geojsonDirFile = file(params.geojson_dir)
    if (!geojsonDirFile.exists()) {
        error "GeoJSON directory does not exist: ${params.geojson_dir}"
    }

    def scriptParam = params.script.toString()
    def scriptCandidates = [
        file(scriptParam),
        file("${projectDir}/${scriptParam}")
    ]
    def scriptFile = scriptCandidates.find { candidate -> candidate.exists() }
    if (!scriptFile) {
        error "Groovy script does not exist: ${params.script} (tried: ${scriptCandidates*.toString().join(', ')})"
    }

    def clearExistingParam = params.get('clear_existing', true) as boolean
    def filePatternParam = params.get('file_pattern', '{stem}.geojson').toString()
    def resolveHierarchyParam = params.get('resolve_hierarchy', true) as boolean

    // Build regex to extract image stems from GeoJSON filenames.
    // E.g. "{stem}.geojson" → regex "(.+)\.geojson"
    def stemPattern = filePatternParam
        .replace('.', '\\.')
        .replace('{stem}', '(.+)')

    // Scan geojson_dir for GeoJSON files, extract one stem per file,
    // then launch a parallel QuPath process per image.
    // NOTE: Channel.fromPath glob can silently return empty on some NFS/VAST
    // filesystems; File.listFiles() is more reliable in that environment.
    def geojsonFiles = geojsonDirFile.listFiles()
        ?.findAll { f -> f.name.endsWith('.geojson') || f.name.endsWith('.geojson.gz') }
        ?: []

    if (geojsonFiles.isEmpty()) {
        error "No .geojson or .geojson.gz files found in: ${params.geojson_dir}"
    }

    Channel
        .from(geojsonFiles)
        .map { f ->
            // Try matching against the original filename first (e.g. file_pattern = "{stem}.geojson.gz"),
            // then fall back to the .gz-stripped name (e.g. file_pattern = "{stem}.geojson").
            def m = (f.name =~ /${stemPattern}/)
            if (m.matches()) return m.group(1)
            def stripped = f.name.replaceAll(/\.gz$/, '')
            def m2 = (stripped =~ /${stemPattern}/)
            m2.matches() ? m2.group(1) : null
        }
        .filter { it != null }
        .unique()
        .map { stem ->
            tuple(
                projectFile.toString(),
                qupathExe.toString(),
                scriptFile.toString(),
                geojsonDirFile.toString(),
                clearExistingParam,
                filePatternParam,
                resolveHierarchyParam,
                stem
            )
        }
        | IMPORT_LARGE_GEOJSON
}
