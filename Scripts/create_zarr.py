#!/usr/bin/env python3
"""
Create a Zarr dataset from GRIB files containing forecast and analysis variables.

Forecast variables:
    - tp: total precipitation (accumulated, steps 0-1, 0-2, ..., 0-48)
    - 2t, 2d: 2m temperature and dewpoint temperature
    - 10u, 10v: 10m wind components
    - u, v: 100m wind components
    - swvl1-4: volumetric soil water layers
    - Hourly steps from 0 to 48

Analysis variables:
    - slt, lsm, cl, z: soil type, land-sea mask, lake cover, geopotential
    - Step 0 only

All variables on the same regular lat/lon grid.
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pygrib
import zarr
from typing import List, Dict, Optional


# Configuration: define variables to extract and process
# Forecast variables have multiple time steps, analysis variables are static
FORECAST_VARS = [
    'tp', '2t', '2d', '10u', '10v', 'u', 'v',
    'swvl1', 'swvl2', 'swvl3', 'swvl4'
]
ANALYSIS_VARS = ['slt', 'lsm', 'cl', 'z']
FORECAST_STEPS = list(range(0, 49))  # 0 to 48 hours

# Variable name mapping (GRIB shortName to standard name)
VAR_NAME_MAP = {
    'tp': 'total_precipitation',
    '2t': '2m_temperature',
    '2d': '2m_dewpoint_temperature',
    '10u': '10m_u_component_of_wind',
    '10v': '10m_v_component_of_wind',
    'u': '100m_u_component_of_wind',
    'v': '100m_v_component_of_wind',
    'swvl1': 'volumetric_soil_water_layer_1',
    'swvl2': 'volumetric_soil_water_layer_2',
    'swvl3': 'volumetric_soil_water_layer_3',
    'swvl4': 'volumetric_soil_water_layer_4',    
    'slt': 'soil_type',
    'lsm': 'land_sea_mask',
    'cl': 'lake_cover',
    'z': 'geopotential'
}


def parse_date(date_str: str) -> datetime:
    """
    Parse date string in YYYYMMDD or YYYY-MM-DD format.

    Parameters
    ----------
    date_str : str
        Date string in format YYYYMMDD or YYYY-MM-DD.

    Returns
    -------
    datetime
        Parsed datetime object.
    """
    date_str = date_str.replace('-', '')
    return datetime.strptime(date_str, '%Y%m%d')


def generate_date_range(
    start_date: datetime,
    end_date: datetime
) -> List[datetime]:
    """
    Generate list of dates from start to end (inclusive).

    Parameters
    ----------
    start_date : datetime
        Start date of the range.
    end_date : datetime
        End date of the range.

    Returns
    -------
    List[datetime]
        List of daily datetime objects from start to end.
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def read_grid_info(grib_file: Path) -> Dict:
    """
    Extract grid information from the first GRIB file.

    Parameters
    ----------
    grib_file : Path
        Path to a GRIB file.

    Returns
    -------
    Dict
        Dictionary containing grid information including lat/lon arrays,
        dimensions, and grid metadata.
    """
    print(f"Reading grid information from {grib_file}")

    with pygrib.open(str(grib_file)) as grbs:  # type: ignore
        grb = grbs[1]  # First message
        lats, lons = grb.latlons()

        # Extract basic grid information
        grid_info = {
            'lats': lats[:, 0],  # Latitude values (1D)
            'lons': lons[0, :],  # Longitude values (1D)
            'nlat': lats.shape[0],  # Number of latitude points
            'nlon': lons.shape[1],  # Number of longitude points
            'grid_type': grb.gridType,  # Grid type (regular_ll, etc.)
        }

        # Try to extract additional grid resolution metadata
        # These attributes may not be available for all GRIB formats
        try:
            grid_info['latitudeOfFirstGridPointInDegrees'] = (
                grb.latitudeOfFirstGridPointInDegrees
            )
            grid_info['longitudeOfFirstGridPointInDegrees'] = (
                grb.longitudeOfFirstGridPointInDegrees
            )
            grid_info['latitudeOfLastGridPointInDegrees'] = (
                grb.latitudeOfLastGridPointInDegrees
            )
            grid_info['longitudeOfLastGridPointInDegrees'] = (
                grb.longitudeOfLastGridPointInDegrees
            )
            grid_info['iDirectionIncrementInDegrees'] = (
                grb.iDirectionIncrementInDegrees
            )
            grid_info['jDirectionIncrementInDegrees'] = (
                grb.jDirectionIncrementInDegrees
            )
        except AttributeError:
            pass

    return grid_info


def initialize_zarr_store(
    output_path: Path,
    grid_info: Dict,
    dates: List[datetime],
    chunk_time: int,
    chunk_lat: int,
    chunk_lon: int,
    compression: str,
    compression_level: int
) -> zarr.Group:
    """
    Initialize Zarr store with dimensions and coordinate variables.

    Creates the zarr directory structure with all coordinate arrays and
    data variables for both forecast and analysis variables.

    Parameters
    ----------
    output_path : Path
        Output directory path for the Zarr store.
    grid_info : Dict
        Grid information dictionary from read_grid_info.
    dates : List[datetime]
        List of dates to process.
    chunk_time : int
        Chunk size along time dimension.
    chunk_lat : int
        Chunk size along latitude dimension (None for no chunking).
    chunk_lon : int
        Chunk size along longitude dimension (None for no chunking).
    compression : str
        Compression algorithm name.
    compression_level : int
        Compression level.

    Returns
    -------
    zarr.Group
        Root zarr group with all arrays initialized.
    """
    print(f"Initializing Zarr store at {output_path}")

    nlat = grid_info['nlat']
    nlon = grid_info['nlon']
    ntime = len(dates)
    nstep = len(FORECAST_STEPS)

    # Handle None chunking (no chunking = full dimension)
    if chunk_lat is None:
        chunk_lat = nlat
    if chunk_lon is None:
        chunk_lon = nlon
    
    # Create root group with directory store
    store = zarr.DirectoryStore(str(output_path))
    root = zarr.group(store=store, overwrite=True)

    # Configure compression algorithm
    # Zstd generally provides better compression ratios
    # Blosc provides faster read/write speeds
    if compression:
        from numcodecs import Zstd, Blosc
        if compression == 'zstd':
            compressor = Zstd(level=compression_level)
        else:
            compressor = Blosc(cname=compression, clevel=compression_level)
    else:
        compressor = None
    
    # Create coordinate arrays with CF-compliant attributes
    # Time coordinate
    root.create_dataset(
        'time',
        shape=(ntime,),
        chunks=(chunk_time,),
        dtype='datetime64[ns]',
        compressor=compressor
    )
    root['time'][:] = np.array([np.datetime64(d) for d in dates])
    root['time'].attrs['long_name'] = 'time'
    root['time'].attrs['standard_name'] = 'time'

    # Forecast step coordinate (hours from forecast initialization)
    root.create_dataset(
        'step',
        shape=(nstep,),
        chunks=(nstep,),
        dtype='i4',
        compressor=compressor
    )
    root['step'][:] = FORECAST_STEPS
    root['step'].attrs['long_name'] = 'forecast step'
    root['step'].attrs['units'] = 'hours'

    # Latitude coordinate
    root.create_dataset(
        'latitude',
        shape=(nlat,),
        chunks=(nlat,),
        dtype='f4',
        compressor=compressor
    )
    root['latitude'][:] = grid_info['lats']
    root['latitude'].attrs['long_name'] = 'latitude'
    root['latitude'].attrs['units'] = 'degrees_north'
    root['latitude'].attrs['standard_name'] = 'latitude'

    # Longitude coordinate
    root.create_dataset(
        'longitude',
        shape=(nlon,),
        chunks=(nlon,),
        dtype='f4',
        compressor=compressor
    )
    root['longitude'][:] = grid_info['lons']
    root['longitude'].attrs['long_name'] = 'longitude'
    root['longitude'].attrs['units'] = 'degrees_east'
    root['longitude'].attrs['standard_name'] = 'longitude'

    # Create data arrays for forecast variables
    # Shape: (time, step, lat, lon) - 4D arrays
    # Includes hourly forecasts for each initialization time
    for var in FORECAST_VARS:
        var_name = VAR_NAME_MAP.get(var, var)
        root.create_dataset(
            var_name,
            shape=(ntime, nstep, nlat, nlon),
            chunks=(chunk_time, min(chunk_time, nstep), chunk_lat, chunk_lon),
            dtype='f4',
            compressor=compressor,
            fill_value=np.nan
        )
        root[var_name].attrs['short_name'] = var
        root[var_name].attrs['coordinates'] = 'time step latitude longitude'
        root[var_name].attrs['_ARRAY_DIMENSIONS'] = [
            'time', 'step', 'latitude', 'longitude'
        ]

    # Create data arrays for analysis variables
    # Shape: (time, lat, lon) - 3D arrays
    # Static fields that don't vary with forecast step
    for var in ANALYSIS_VARS:
        var_name = VAR_NAME_MAP.get(var, var)
        root.create_dataset(
            var_name,
            shape=(ntime, nlat, nlon),
            chunks=(chunk_time, chunk_lat, chunk_lon),
            dtype='f4',
            compressor=compressor,
            fill_value=np.nan
        )
        root[var_name].attrs['short_name'] = var
        root[var_name].attrs['coordinates'] = 'time latitude longitude'
        root[var_name].attrs['_ARRAY_DIMENSIONS'] = [
            'time', 'latitude', 'longitude'
        ]

    # Add global metadata attributes following CF conventions
    root.attrs['Conventions'] = 'CF-1.8'
    root.attrs['title'] = 'GRIB data converted to Zarr'
    root.attrs['creation_date'] = datetime.now().isoformat()
    root.attrs['grid_type'] = grid_info.get('grid_type', 'unknown')

    return root


def extract_variable_from_grib(
    grib_file: Path,
    var_shortname: str,
    step: Optional[int] = None,
) -> np.ndarray:
    """
    Extract a single variable from GRIB file.

    Parameters
    ----------
    grib_file : Path
        Path to GRIB file.
    var_shortname : str
        GRIB short name of the variable.
    step : Optional[int]
        Forecast step in hours (None for analysis variables).

    Returns
    -------
    np.ndarray
        2D array of variable values.

    Raises
    ------
    ValueError
        If variable not found in GRIB file.
    """
    with pygrib.open(str(grib_file)) as grbs:  # type: ignore
        # Build selection criteria based on variable type
        if step is not None:
            # For forecast variables with specific step
            selected = grbs.select(shortName=var_shortname, stepRange=str(step))
            if not selected:
                # Try without stepRange for step 0
                selected = grbs.select(shortName=var_shortname, step=step)
        else:
            # For analysis variables
            selected = grbs.select(shortName=var_shortname)
        
        if not selected:
            raise ValueError(f"Variable {var_shortname} (step={step}) not found in {grib_file}")
        
        # Return the first match
        return selected[0].values


def extract_all_steps_from_grib(
    grib_file: Path,
    var_shortname: str,
    steps: List[int],
    nx: int = 0,
    ny: int = 0
) -> np.ndarray:
    """
    Extract all forecast steps for a variable from GRIB file.

    This function handles both instantaneous and accumulated variables.
    For total precipitation (tp), uses endStep to get accumulated values.

    Parameters
    ----------
    grib_file : Path
        Path to GRIB file.
    var_shortname : str
        GRIB short name of the variable.
    steps : List[int]
        List of forecast steps to extract.
    nx : int
        Number of latitude points (for zero initialization).
    ny : int
        Number of longitude points (for zero initialization).

    Returns
    -------
    np.ndarray
        3D array with shape (nsteps, nlat, nlon).

    Raises
    ------
    ValueError
        If variable/step combination not found in GRIB file.
    """
    data_list = []
    with pygrib.open(str(grib_file)) as grbs:  # type: ignore
        for step in steps:
            # Special handling for accumulated precipitation at step 0
            # Total precipitation is zero at initialization time
            if step == 0 and var_shortname == 'tp':
                data_list.append(np.zeros((nx, ny)))
                continue

            # Select GRIB message based on variable type
            if var_shortname == 'tp':
                # For accumulated variables, use endStep
                selected = grbs.select(
                    shortName=var_shortname,
                    endStep=step
                )
            else:
                # For instantaneous variables, use stepRange
                selected = grbs.select(
                    shortName=var_shortname,
                    stepRange=str(step)
                )

            if not selected:
                # Fallback: try with step parameter
                selected = grbs.select(
                    shortName=var_shortname,
                    step=step
                )

            if not selected:
                raise ValueError(
                    f"Variable {var_shortname} step {step} "
                    f"not found in {grib_file}"
                )

            data_list.append(selected[0].values)

    return np.stack(data_list, axis=0)


def process_date(
    date: datetime,
    date_idx: int,
    grib_file: Path,
    root: zarr.Group,
    nx: int = 0,
    ny: int = 0
) -> None:
    """
    Process a single date's GRIB file and write data to Zarr store.

    Extracts all forecast and analysis variables from the GRIB file
    and writes them to the appropriate location in the Zarr arrays.

    Parameters
    ----------
    date : datetime
        Date being processed.
    date_idx : int
        Index of this date in the time dimension.
    grib_file : Path
        Path to GRIB file for this date.
    root : zarr.Group
        Root zarr group to write data to.
    nx : int
        Number of latitude points.
    ny : int
        Number of longitude points.

    Returns
    -------
    None
    """
    print(f"Processing {date.strftime('%Y-%m-%d')} [{date_idx+1}]")

    if not grib_file.exists():
        print(f"  WARNING: File not found: {grib_file}")
        return

    # Process forecast variables (all time steps)
    # These include hourly forecasts from 0 to 48 hours (all time steps)
    # These include hourly forecasts from 0 to 48 hours
    for var in FORECAST_VARS:
        var_name = VAR_NAME_MAP.get(var, var)
        print(f"  {var_name}...", end=' ', flush=True)

        try:
            # Read all forecast steps at once for efficiency
            # This is faster than reading steps individually
            data = extract_all_steps_from_grib(
                grib_file, var, FORECAST_STEPS, nx, ny
            )
            # Write all steps at once to zarr
            root[var_name][date_idx, :, :, :] = data
            print("done")
        except Exception as e:
            print(f"\n    WARNING: Could not extract {var}: {e}")

    # Process analysis variables (step 0 only)
    # These are static fields that don't vary with forecast step
    for var in ANALYSIS_VARS:
        var_name = VAR_NAME_MAP.get(var, var)
        print(f"  {var_name}...", end=' ', flush=True)
        
        try:
            data = extract_variable_from_grib(grib_file, var)
            root[var_name][date_idx, :, :] = data
            print("done")
        except Exception as e:
            print(f"WARNING: Could not extract {var}: {e}")


def main(args) -> None:
    """
    Main function to orchestrate Zarr dataset creation.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        If GRIB directory or files are not found.
    """
    # Parse and validate date range
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    dates = generate_date_range(start_date, end_date)

    print(f"Creating Zarr dataset for {len(dates)} dates")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to "
          f"{end_date.strftime('%Y-%m-%d')}")

    # Setup input and output paths
    grib_dir = Path(args.grib_dir)
    output_path = Path(args.output)

    if not grib_dir.exists():
        raise FileNotFoundError(f"GRIB directory not found: {grib_dir}")

    # Find first available GRIB file to extract grid metadata
    first_grib = None
    for date in dates:
        grib_file = grib_dir / args.grib_pattern.format(date=date.strftime('%Y%m%d'))
        if grib_file.exists():
            first_grib = grib_file
            break
    
    if first_grib is None:
        raise FileNotFoundError(
            "No GRIB files found in the specified date range"
        )

    # Read grid information from first available file
    # All files are assumed to have the same grid structure
    grid_info = read_grid_info(first_grib)
    nx = grid_info['nlat']
    ny = grid_info['nlon']
    print(f"Grid: {nx} x {ny} ({grid_info['grid_type']})")

    # Initialize Zarr store with all dimensions and variables
    root = initialize_zarr_store(
        output_path,
        grid_info,
        dates,
        args.chunk_time,
        args.chunk_lat,
        args.chunk_lon,
        args.compression,
        args.compression_level
    )

    # Process each date's GRIB file and populate zarr arrays
    for date_idx, date in enumerate(dates):
        grib_file = grib_dir / args.grib_pattern.format(
            date=date.strftime('%Y%m%d')
        )
        process_date(date, date_idx, grib_file, root, nx, ny)

    # Print summary information
    print(f"\nZarr dataset created successfully at {output_path}")
    print("Dataset info:")
    print(f"  Forecast variables: {', '.join(FORECAST_VARS)}")
    print(f"  Analysis variables: {', '.join(ANALYSIS_VARS)}")
    print(f"  Time dimension: {len(dates)}")
    print(f"  Forecast steps: {len(FORECAST_STEPS)} (0-48 hours)")
    print(f"  Spatial dimensions: {nx} x {ny}")


if __name__ == '__main__':
    # Get current date as default for date arguments
    today = datetime.now().strftime('%Y%m%d')

    # Setup command-line argument parser
    parser = argparse.ArgumentParser(
        description='Create Zarr dataset from GRIB files'
    )
    parser.add_argument(
        '--run_where',
        type=str,
        default='local',
        choices=['local', 'leonardo'],
        help='Environment where the code is running (default: leonardo)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=today,
        help='Start date (YYYYMMDD or YYYY-MM-DD, default: today)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=today,
        help='End date (YYYYMMDD or YYYY-MM-DD, default: today)'
    )
    parser.add_argument(
        '--grib-dir',
        type=str,
        default=None,
        help='Directory containing GRIB files (default: depends on environment)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output Zarr dataset path (default: depends on environment)'
    )
    parser.add_argument(
        '--grib-pattern',
        type=str,
        default='edt_{date}.grib',
        help='GRIB filename pattern (use {date} placeholder for YYYYMMDD)'
    )
    parser.add_argument(
        '--chunk-time',
        type=int,
        default=1,
        help='Chunk size along time dimension'
    )
    parser.add_argument(
        '--chunk-lat',
        type=int,
        default=None,
        help='Chunk size along latitude dimension (default: no chunking)'
    )
    parser.add_argument(
        '--chunk-lon',
        type=int,
        default=None,
        help='Chunk size along longitude dimension (default: no chunking)'
    )
    parser.add_argument(
        '--compression',
        type=str,
        default='zstd',
        choices=['zstd', 'gzip', 'bz2', 'lz4', None],
        help='Compression algorithm'
    )
    parser.add_argument(
        '--compression-level',
        type=int,
        default=3,
        help='Compression level'
    )
    
    args = parser.parse_args()

    # Set environment-specific defaults for paths
    # Leonardo: HPC cluster paths
    # Local: relative paths in current directory
    if args.grib_dir is None:
        if args.run_where == 'leonardo':
            args.grib_dir = (
                '/leonardo_scratch/fast/DE374_lot2/extremes-dt_lumi'
            )
        else:  # local
            args.grib_dir = './grib_file'

    if args.output is None:
        if args.run_where == 'leonardo':
            args.output = (
                '/leonardo_scratch/fast/DE374_lot2/zarr_deliv/'
                'DE374h_deliv.zarr'
            )
        else:  # local
            args.output = './zarr_file/DE374h_deliv.zarr'

    main(args)
