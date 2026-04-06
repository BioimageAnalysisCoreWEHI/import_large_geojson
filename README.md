# import_large_geojson

Nextflow pipeline for importing large GeoJSON cell annotations into a QuPath project using headless QuPath.

## What it does

- Runs QuPath in CLI mode against a `.qpproj` project.
- Executes `import_large_geojson.groovy` to match and import GeoJSON files into each image in the project.
- Each GeoJSON file is matched to a project image by filename stem (e.g. `image_001.geojson` → `image_001.ome.tif`).
- Optionally clears existing objects before importing (default: yes).

## Required parameters

- `--project` Path to QuPath project (`.qpproj`).
- `--geojson_dir` Directory containing GeoJSON files to import.

## Optional parameters

- `--qupath_bin` Path to QuPath executable (default: `/stornext/System/data/software/rhel/9/base/tools/QuPath/0.6.0/bin/QuPath`).
- `--script` Groovy script path (default: `bin/import_large_geojson.groovy`).
- `--clear_existing` Clear all existing objects before importing (default: `true`).
- `--file_pattern` Pattern for matching GeoJSON files to images (default: `{stem}.geojson`). `{stem}` is replaced with the image name without extension.
- `--outdir` Output directory for log files (default: `results`).
- `--publish_dir_mode` Nextflow `publishDir` mode (default: `copy`).

## Usage

```bash
nextflow run main.nf \
    --project /path/to/QuPath_project/project.qpproj \
    --geojson_dir /path/to/geojson_files/ \
    --outdir /path/to/output
```

With custom file pattern (e.g. for files named `sample_segmentation.geojson`):

```bash
nextflow run main.nf \
    --project /path/to/QuPath_project/project.qpproj \
    --geojson_dir /path/to/geojson_files/ \
    --file_pattern "{stem}_segmentation.geojson"
```

To keep existing objects (append mode):

```bash
nextflow run main.nf \
    --project /path/to/QuPath_project/project.qpproj \
    --geojson_dir /path/to/geojson_files/ \
    --clear_existing false
```

## Resource profiles

Memory is the main resource for large GeoJSON imports. CPU count is low since QuPath imports are single-threaded.

| Profile | Executor | Queue | CPUs | Memory | Time |
|---|---|---|---:|---:|---:|
| `standard` (default) | slurm | regular | 4 | 450 GB | 48h |
| `small` | slurm | regular | 4 | 128 GB | 48h |
| `medium` | slurm | regular | 4 | 480 GB | 48h |
| `large` | slurm | regular | 8 | 1200 GB | 48h |

Use `-profile small`, `-profile medium`, or `-profile large` to override.

## Outputs

- `qupath_geojson_import.log` with full QuPath run logs including per-image import status.

## Notes

- For large GeoJSON files (100GB+), use the `medium` or `large` profile to ensure enough memory.
- The Groovy script handles `.ome.tif` / `.ome.tiff` extensions when matching stems.
- Images with no matching GeoJSON file are skipped with a warning (not an error).
