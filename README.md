# import_large_geojson

Nextflow pipeline for importing large GeoJSON cell annotations into a QuPath project using headless QuPath.

## What it does

- Scans `geojson_dir` for GeoJSON files, extracts image stems from the filenames, and launches **one parallel process per image**.
- Each process runs QuPath in CLI mode against the `.qpproj` project, importing only the matched image's GeoJSON.
- GeoJSON files are matched to project images by filename stem (e.g. `image_001.geojson` → `image_001.ome.tif`).
- Optionally clears existing objects before importing (default: yes).
- On SLURM, each image gets its own job — a project with 10 images runs 10 concurrent SLURM jobs instead of one sequential job.

## Required parameters

- `--project` Path to QuPath project (`.qpproj`).
- `--geojson_dir` Directory containing GeoJSON files to import.

## Optional parameters

- `--qupath_bin` Path to QuPath executable (default: `/stornext/System/data/software/rhel/9/base/tools/QuPath/0.6.0/bin/QuPath`).
- `--script` Groovy script path (default: `bin/import_large_geojson.groovy`).
- `--clear_existing` Clear all existing objects before importing (default: `true`).
- `--file_pattern` Pattern for matching GeoJSON files to images (default: `{stem}.geojson`). `{stem}` is replaced with the image name without extension.
- `--resolve_hierarchy` Run QuPath `resolveHierarchy()` after import (default: `true`). Set `false` for flat detections to skip the O(n²) nesting step — major speedup.
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

Memory is the main resource for large GeoJSON imports.

| Profile | Executor | Queue | CPUs | Memory (per attempt) | Time |
|---|---|---|---:|---:|---:|
| `standard` (default) | slurm | regular | 8 | 240 GB × attempt | 48h |
| `small` | slurm | regular | 8 | 50 GB × attempt | 48h |
| `medium` | slurm | regular | 14 | 240 GB × attempt | 48h |
| `large` | slurm | regular | 16 | 700 GB × attempt | 48h |

Use `-profile small`, `-profile medium`, or `-profile large` to override.

> **Tip:** Adding `--resolve_hierarchy false` significantly reduces both peak memory and runtime when importing flat detections (no annotation nesting). You may be able to use a smaller profile with this flag.

## Outputs

- One log file per image: `qupath_geojson_import_<stem>.log` with full QuPath import status.

## Notes

- The pipeline now runs **one SLURM job per image** for full parallelism. With 10 GeoJSON files you get 10 concurrent imports.
- For large GeoJSON files (100GB+), use the `medium` or `large` profile to ensure enough memory.
- If your GeoJSON contains only flat detections (no annotation nesting), set `--resolve_hierarchy false` to skip the expensive O(n²) hierarchy resolution step.
- The Groovy script handles `.ome.tif` / `.ome.tiff` extensions when matching stems.
- Images with no matching GeoJSON file in `geojson_dir` are simply not processed (no wasted jobs).
