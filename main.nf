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
    Channel
        .fromPath(["${geojsonDirFile}/*.geojson", "${geojsonDirFile}/*.geojson.gz"])
        .map { f ->
            def name = f.name.replaceAll(/\.gz$/, '')
            def m = (name =~ /${stemPattern}/)
            m.matches() ? m.group(1) : null
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
