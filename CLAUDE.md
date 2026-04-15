# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **ECMWF LOT2 Data Procurement** project for the **Destination Earth (DestinE)** initiative. It downloads and processes climate and extreme weather forecast data from the DestinE Data Lake via Polytope API, MARS, and HDA access methods.

## Repository Structure

- **Scripts/**: Operational data download tools (the main production code)
  - `DE374h_download.py` — Main download orchestrator for IFS (MARS) and Extremes-DT (Polytope) data
  - `c_api_request.py` — API request builders for Polytope and MARS services
  - `c_directories.py` — Path configuration for different execution environments (local, Leonardo HPC, ATOS)
  - `create_zarr.py` — GRIB-to-Zarr conversion utility (Blosc/Zstd compression, per-model native grids)
  - `mars_lumi.req` — MARS request template for extremes-dt retrieval on Lumi
  - **SLURM job files (Leonardo HPC):**
    - `crontab_ifs.job` — Daily IFS download (3 days ago), self-rescheduling at 11:00 next day
    - `crontab_edt.job` — Daily EDT download (yesterday), self-rescheduling at 11:00 next day; loads eccodes module
    - `crontab_ifs_hist.job` — Historical IFS backfill, chains backward day-by-day down to 2024-06-01
    - `crontab_edt_hist.job` — Historical EDT backfill, chains backward day-by-day down to 2024-05-30
    - `extremes_dt.job` — Array job (5-day batches) for EDT bulk downloads
    - `aim.job` — Historical IFS bulk download via direct MARS requests (2020–2025)
  - `extremes-dt_crontab.sh` — Bash wrapper for scheduled execution on ATOS
- **polytope-examples/**: Reference examples and Jupyter notebooks for accessing DestinE Digital Twin data via Polytope
  - `desp-authentication.py` — OAuth2 token retrieval, stores credentials in `~/.polytopeapirc`
  - Subdirectories: `climate-dt/`, `extremes-dt/`, `on-demand-extremes-dt/`, `nextgems/`
- **DestinE-DataLake-Lab/**: EUMETSAT reference notebooks (HDA, HOOK, STACK services)
- **extremes-dt/**: Output directory for downloaded GRIB files (`grib/` and `temp/` subdirectories)

## Key Commands

### Environment Setup

```bash
# Create conda environment
envname=earthkit
conda create -n $envname -c conda-forge -y python=3.10
conda env update -n $envname -f polytope-examples/environment.yml
conda activate $envname

# Or pip install
pip install -r polytope-examples/requirements.txt
pip install --upgrade polytope-client lxml conflator
```

### Authentication

```bash
python polytope-examples/desp-authentication.py -u <username> -p <password>
# Stores token in ~/.polytopeapirc
```

### Running Data Downloads

```bash
# Local extremes-dt download
python Scripts/DE374h_download.py --nwp edt --run_where local --date_i 20250101 --date_f 20250131

# Local IFS download
python Scripts/DE374h_download.py --nwp ifs --run_where local --date_i 20250101 --date_f 20250131

# Leonardo HPC — daily self-rescheduling jobs (uses uv run)
sbatch Scripts/crontab_ifs.job      # IFS daily (downloads 3 days ago, reschedules tomorrow 11:00)
sbatch Scripts/crontab_edt.job      # EDT daily (downloads yesterday, reschedules tomorrow 11:00)

# Leonardo HPC — historical backfill (chains backward automatically)
sbatch Scripts/crontab_ifs_hist.job  # IFS backfill down to 2024-06-01
sbatch Scripts/crontab_edt_hist.job  # EDT backfill down to 2024-05-30

# Leonardo HPC — EDT bulk array job (5-day batches)
START_DATE=2025-04-01 sbatch Scripts/extremes_dt.job
```

## Architecture Notes

### Data Flow
1. Authenticate via DESP OAuth2 → token in `~/.polytopeapirc`
2. Build request dicts (date, parameters, area) via `c_api_request.py`
3. Download via `earthkit.data.from_source()` (Polytope or MARS)
4. Post-process GRIB files (e.g., precipitation step range correction for EDT)
5. Merge parameter files using `grib_copy`
6. Store in date-specific paths configured by `c_directories.py`

### Multi-Environment Support
`c_directories.py` adapts paths for three environments:
- **local**: User-specific paths
- **leonardo**: CINECA Leonardo HPC (`/leonardo_scratch/`, `/leonardo_work/`)
- **atos** (now ita3494): ECMWF systems

### EDT Parameter History
- Pre-2025-02-05: Uses satellite-derived cloud parameters (shortNames 228246, 228247)
- Post-2025-02-05: Uses model-derived parameters (131, 132)

### Geographic Coverage
Bounding box: 70.5°N to 29.5°N, 23.5°W to 62.5°E (Scandinavia to North Africa/Middle East)

## Key Dependencies

- **earthkit** (≥0.11.2): High-level data access
- **polytope-client**: Polytope API client
- **eccodes**: GRIB file manipulation (Leonardo module: `eccodes/2.33.0--intel-oneapi-mpi--2021.12.1--oneapi--2024.1.0`)
- **healpy**: HEALPix grid operations
- **xarray**: Multidimensional arrays (via earthkit)
- **zarr** / **pygrib** / **numcodecs**: Used by `create_zarr.py` for Zarr dataset creation
- **uv**: Used as the Python runner on Leonardo (`uv run`)

## API Constraints

- Polytope rate limit: 50 requests/second, max 5 concurrent operations
- Polytope endpoint: `polytope.lumi.apps.dte.destination-earth.eu`

## Git Repository

- Remote: `git@github.com:meteocima/DE374h_WP1.git`
- Branches: `main`, `lmonaco_0.0.1`
