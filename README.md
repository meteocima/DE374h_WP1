# DE374 LOT2 - Weather Data Procurement System

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![ECMWF](https://img.shields.io/badge/data-ECMWF-green.svg)](https://www.ecmwf.int/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A comprehensive system for downloading and processing weather forecast data from ECMWF sources, designed for the DE374h project. Supports multiple data sources including IFS operational forecasts and Extremes-DT high-resolution data.

## 🌟 Features

- **Multi-source Data Access**: Download from ECMWF IFS and Extremes-DT datasets
- **Flexible Environment Support**: Works on local machines and Leonardo HPC cluster  
- **Automated Scheduling**: Built-in SLURM job scheduling for HPC environments
- **Data Validation**: Comprehensive availability checking and monitoring
- **High Performance**: Optimized for large-scale weather data processing

## 📋 Quick Start

### Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) package manager (recommended) or pip
- ECMWF API credentials (for MARS access)
- Polytope credentials (for Extremes-DT access)

### Installation

1. **Clone the repository**
   ```bash
   # HTTPS (recommended for most users)
   git clone https://github.com/meteocima/DE374h_WP1.git
   cd DE374h_WP1
   
   # SSH (for users with SSH keys configured)
   git clone git@github.com:meteocima/DE374h_WP1.git
   cd DE374h_WP1
   ```

2. **Install dependencies**
   ```bash
   # Using uv (recommended)
   uv sync
   
   # Or using pip
   pip install -e .
   ```

3. **Configure API credentials**
   - Set up ECMWF API key for MARS access
   - Configure Polytope credentials for Destination Earth platform

## 🚀 Usage

### Basic Data Download

**Local Development - Download Current Day:**
For testing and development on your local machine, download today's IFS operational data:

```bash
uv run Scripts/DE374h_download.py --nwp ifs --run_where local
```

**HPC Production - Download Date Range:**
For production runs on Leonardo HPC, specify a date range to download multiple days of IFS data:

```bash
uv run Scripts/DE374h_download.py --nwp ifs --run_where leonardo --date_i 20241201 --date_f 20241205
```

**High-Resolution Data - Extremes-DT:**
Download high-resolution Extremes-DT data (0.04° resolution) for specific dates:

```bash
uv run Scripts/DE374h_download.py --nwp edt --run_where leonardo --date_i 20241215 --date_f 20241215
```

### Command Line Options

| Option | Description | Values | Default |
|--------|-------------|---------|---------|
| `--nwp` | NWP model type | `ifs`, `edt` | `ifs` |
| `--run_where` | Computing environment | `local`, `leonardo` | `local` |
| `--date_i` | Start date | `YYYYMMDD` | Today |
| `--date_f` | End date | `YYYYMMDD` | Today |

### HPC Scheduling

**Daily IFS Operational Data:**
Schedule automated daily IFS data download with self-recurring job scheduling:

```bash
sbatch Scripts/crontab_ifs.job
```

**Historical IFS Data:**
Download historical IFS data recursively backward in time. Starts from the given date and works backward day by day until the stop date defined in the job file:

```bash
sbatch Scripts/crontab_ifs_hist.job 20241201
```

**Daily Extremes-DT Operational Data:**
Schedule automated daily Extremes-DT high-resolution data download:

```bash
sbatch Scripts/crontab_edt.job
```

**Historical Extremes-DT Data:**
Download historical Extremes-DT data recursively backward in time. Starts from the given date and works backward day by day until the stop date defined in the job file:

```bash
sbatch Scripts/crontab_edt_hist.job 20241201
```

## 📁 Project Structure

```
DE374_lot2/
├── README.md                          # This file
├── LICENSE                           # MIT License
├── pyproject.toml                    # UV Project configuration and dependencies
├── edt_polytope_availability.csv    # Polytope data availability (04/04/2024 - 22/11/2025)
└── Scripts/                          # Main application scripts
    ├── DE374h_download.py           # Main download script
    ├── c_api_request.py             # API request handling
    ├── c_directories.py             # Directory management
    ├── create_zarr.py               # Data format conversion
    ├── polytope_check.ipynb         # Data availability notebook
    └── crontab_*.job               # SLURM job scripts
```

## 📊 Data Sources

### ECMWF IFS (Integrated Forecasting System)
- **Resolution**: 0.1° (~10km)
- **Forecast Range**: 0-48 hours
- **Parameters**: Temperature, wind, precipitation, soil moisture, etc.
- **Access**: MARS API

### Extremes-DT (Destination Earth)
- **Resolution**: 0.04° (~4km) 
- **Forecast Range**: 0-48 hours
- **Parameters**: High-resolution weather variables
- **Access**: Polytope API

## 🗺️ Geographic Coverage

- **Domain**: Europe region
- **Bounding Box**: 70.5°N / -23.5°W / 29.5°N / 62.5°E
- **Includes**: Continental Europe, British Isles, Scandinavia

## 🔧 Configuration

### Environment-Specific Paths

The system automatically configures storage paths based on the environment:

- **Local**: `~/CIMA/202508_ECMWF_LOT2_DATAPROCUREMENT/`
- **Leonardo HPC**: 
  - IFS Data: `/leonardo_work/DE374_lot2_0/IFS/`
  - Extremes-DT Data: `/leonardo_scratch/fast/DE374_lot2/extremes-dt/`
  - Scratch: `/leonardo_scratch/large/userexternal/lmonaco0/DE374_lot2/`

### Supported Parameters

#### IFS Parameters
- **167.128**: 2m temperature
- **168.128**: 2m dewpoint temperature  
- **165.128**: 10m U wind component
- **166.128**: 10m V wind component
- **228.128**: Total precipitation
- **228246/228247**: 100m wind components
- **39**: Volumetric soil moisture (layer 1)
- **40**: Volumetric soil moisture (layer 2)
- **41**: Volumetric soil moisture (layer 3) 
- **42**: Volumetric soil moisture (layer 4)
- **43**: Soil type
- **172**: Land-sea mask
- **26**: Lake cover
- **129**: Geopotential (surface orography)

#### Extremes-DT Parameters  
- **167**: 2-meter temperature
- **168**: 2-meter dewpoint temperature  
- **165/166**: 10-meter U/V wind components
- **228**: Total precipitation
- **228246/228247**: 100m wind components (until 04/02/2025)
- **131/132**: 100m wind components (after 04/02/2025)

## 📝 Monitoring and Validation

### Data Availability Reports

**Historical Availability Data:**
The project includes a comprehensive availability report for Extremes-DT data:
- **edt_polytope_availability.csv**: Complete availability matrix from Polytope service launch (April 4, 2024) through November 22, 2025
- Shows parameter availability by date for systematic data procurement planning

**Interactive Analysis:**
Use the Jupyter notebook for real-time data availability checking:

```bash
# Launch notebook for interactive checking
jupyter lab Scripts/polytope_check.ipynb
```

The monitoring system provides:
- Historical availability analysis from existing CSV reports
- Real-time parameter-specific availability testing
- Data quality validation and verification
- Sample data download and format checking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For issues and questions:
- Check the [Issues](../../issues) page
- Review the `polytope_check.ipynb` notebook for troubleshooting
- Verify API credentials and network connectivity

## 🏗️ Technical Details

### Dependencies
- **earthkit**: ECMWF data access library
- **eccodes**: GRIB file processing  
- **pandas**: Data analysis and CSV handling
- **pathlib**: Cross-platform file path handling

### Performance Notes
- Extremes-dt is not always homogeneously present, so for this model every variable is downloaded separately and then merged using eccodes
- High-resolution Extremes-DT data availability varies by date and parameter, requiring individual variable downloads that are subsequently merged using eccodes
- Optimized memory management with automatic temporary file cleanup
- Real-time progress monitoring through comprehensive timestamped logging
- Robust error handling and exception management for reliable data processing


---

**Project**: DE374 LOT2 Weather Data Procurement  
**Organization**: CIMA Foundation  
**Contact**: luca.monaco@cimafoundation.org