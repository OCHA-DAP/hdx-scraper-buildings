# AGENTS.md

This file provides guidance when working with code in this repository.

## Commands

```shell
# Setup
uv sync && source .venv/bin/activate

# Run the full scraper
uv run task app         # python run.py (auto-activates venv if needed)
uv run task google      # run only Google provider
uv run task microsoft   # run only Microsoft provider

# Lint
uv run task ruff        # ruff format && ruff check && ruff format

# Tests
uv run pytest           # run all tests
uv run pytest -c pytest.ini --cov hdx  # with coverage

# Pre-commit
uv run pre-commit install
uv run pre-commit run --all-files

# Build
uv build
```

## Architecture

This scraper collects building footprint data from Google and Microsoft Open Buildings datasets and publishes them as HDX datasets.

**Three-phase pipeline** (controlled by env vars):
1. **Download** (`RUN_DOWNLOAD=YES`): Fetch raw files (CSV.gz or GeoJSONL.gz) from provider URLs, convert to GeoParquet via GDAL, upload to S3-compatible storage (`source.coop`)
2. **Group** (`RUN_GROUPING=YES`): Pull parquet files from S3 by country bounding box using DuckDB spatial queries, convert to File Geodatabase (`.gdb.zip`) via GDAL
3. **Package**: Create/update HDX datasets with the zipped GDBs as resources

**Key env vars** (`common/config.py`, loaded from `.env`):
- `RUN_GOOGLE` / `RUN_MICROSOFT` — which providers to run (default: both off)
- `RUN_DOWNLOAD` — run download phase (default: NO)
- `RUN_GROUPING` — run grouping phase (default: YES)
- `ISO3_INCLUDE` / `ISO3_EXCLUDE` — comma-separated ISO3 codes to filter countries
- `CONCURRENCY_LIMIT` — async download parallelism (default: cpu count)

**External data dependencies:**
- ADM0 boundaries: `https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet`
- ADM1 boundaries: `https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet`
- Intermediate storage: S3-compatible `us-west-2.opendata.source.coop`

**Splitting logic** (`common/group.py`): If a country's GDB exceeds 1.5 GB, the country-level file is replaced with per-ADM1 files.

**HDX dataset naming**: `buildings-{provider}-{iso3lower}` — datasets are created/updated via `hdx-python-api`.

**Retry behavior**: Downloads and HDX uploads use `tenacity` (24 attempts, 10s wait — designed for ~1 day of retries).

## Project management

- `uv` manages dependencies and virtual environments
- Add runtime deps to `project.dependencies` in `pyproject.toml`
- Add dev-only deps to `[dependency-groups].dev`
- Docker image published to `public.ecr.aws/unocha/hdx-scraper-buildings` on release
- `compose.yaml` mounts `~/.hdx_configuration.yaml`, `~/.useragents.yaml`, and `./saved_data` for local Docker runs
