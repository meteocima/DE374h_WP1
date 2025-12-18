#!/usr/bin/env python3
"""
Create a Zarr dataset from GRIB files containing forecast and analysis variables.

Forecast variables: 2t, 2d, 10u, 10v, 100u, 100v, swvl1, swvl2, swvl3, swvl4
    - Hourly steps from 0 to 48
Analysis variables: slt, lsm, cl, z
    - Step 0 only

All variables on the same regular lat/lon grid.
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pygrib
import zarr
from typing import List, Dict, Tuple, Optional


# Configuration
FORECAST_VARS = ['2t', '2d', '10u', '10v', '100u', '100v', 'swvl1', 'swvl2', 'swvl3', 'swvl4']
ANALYSIS_VARS = ['slt', 'lsm', 'cl', 'z']
FORECAST_STEPS = list(range(0, 49))  # 0 to 48 hours


# Variable name mapping (GRIB shortName to standard name)
VAR_NAME_MAP = {
    '2t': '2m_temperature',
    '2d': '2m_dewpoint_temperature',
    '10u': '10m_u_component_of_wind',
    '10v': '10m_v_component_of_wind',
    '100u': '100m_u_component_of_wind',
    '100v': '100m_v_component_of_wind',
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
    """Parse date string in YYYYMMDD or YYYY-MM-DD format."""
    date_str = date_str.replace('-', '')
    return datetime.strptime(date_str, '%Y%m%d')


def generate_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
    """Generate list of dates from start to end (inclusive)."""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def read_grid_info(grib_file: Path) -> Dict:
    """Extract grid information from the first GRIB file."""
    print(f"Reading grid information from {grib_file}")
    
    with pygrib.open(str(grib_file)) as grbs:  # type: ignore
        grb = grbs[1]  # First message
        lats, lons = grb.latlons()
        
        grid_info = {
            'lats': lats[:, 0],  # Latitude values (1D)
            'lons': lons[0, :],  # Longitude values (1D)
            'nlat': lats.shape[0],
            'nlon': lons.shape[1],
            'grid_type': grb.gridType,
        }
        
        # Try to get grid resolution info
        try:
            grid_info['latitudeOfFirstGridPointInDegrees'] = grb.latitudeOfFirstGridPointInDegrees
            grid_info['longitudeOfFirstGridPointInDegrees'] = grb.longitudeOfFirstGridPointInDegrees
            grid_info['latitudeOfLastGridPointInDegrees'] = grb.latitudeOfLastGridPointInDegrees
            grid_info['longitudeOfLastGridPointInDegrees'] = grb.longitudeOfLastGridPointInDegrees
            grid_info['iDirectionIncrementInDegrees'] = grb.iDirectionIncrementInDegrees
            grid_info['jDirectionIncrementInDegrees'] = grb.jDirectionIncrementInDegrees
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
    """Initialize Zarr store with dimensions and coordinate variables."""
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
    
    # Create root group
    store = zarr.DirectoryStore(str(output_path))
    root = zarr.group(store=store, overwrite=True)
    
    # Set compression
    if compression:
        from numcodecs import Zstd, Blosc
        if compression == 'zstd':
            compressor = Zstd(level=compression_level)
        else:
            compressor = Blosc(cname=compression, clevel=compression_level)
    else:
        compressor = None
    
    # Create coordinate arrays
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
    
    # Create data arrays for forecast variables (time, step, lat, lon)
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
        root[var_name].attrs['_ARRAY_DIMENSIONS'] = ['time', 'step', 'latitude', 'longitude']
    
    # Create data arrays for analysis variables (time, lat, lon)
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
        root[var_name].attrs['_ARRAY_DIMENSIONS'] = ['time', 'latitude', 'longitude']
    
    # Add global attributes
    root.attrs['Conventions'] = 'CF-1.8'
    root.attrs['title'] = 'GRIB data converted to Zarr'
    root.attrs['creation_date'] = datetime.now().isoformat()
    root.attrs['grid_type'] = grid_info.get('grid_type', 'unknown')
    
    return root


def extract_variable_from_grib(
    grib_file: Path,
    var_shortname: str,
    step: Optional[int] = None
) -> np.ndarray:
    """Extract a variable from GRIB file."""
    with pygrib.open(str(grib_file)) as grbs:  # type: ignore
        # Build selection criteria
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


def process_date(
    date: datetime,
    date_idx: int,
    grib_file: Path,
    root: zarr.Group
) -> None:
    """Process a single date's GRIB file and write to Zarr."""
    print(f"Processing {date.strftime('%Y-%m-%d')} [{date_idx+1}]")
    
    if not grib_file.exists():
        print(f"  WARNING: File not found: {grib_file}")
        return
    
    # Process forecast variables
    for var in FORECAST_VARS:
        var_name = VAR_NAME_MAP.get(var, var)
        print(f"  {var_name}...", end=' ', flush=True)
        
        for step_idx, step in enumerate(FORECAST_STEPS):
            try:
                print(step_idx,step)
                data = extract_variable_from_grib(grib_file, var, step)
                root[var_name][date_idx, step_idx, :, :] = data
            except Exception as e:
                print(f"\n    WARNING: Could not extract {var} step {step}: {e}")
        
        print("done")
    
    # Process analysis variables (step 0 only)
    for var in ANALYSIS_VARS:
        var_name = VAR_NAME_MAP.get(var, var)
        print(f"  {var_name}...", end=' ', flush=True)
        
        try:
            data = extract_variable_from_grib(grib_file, var)
            root[var_name][date_idx, :, :] = data
            print("done")
        except Exception as e:
            print(f"WARNING: Could not extract {var}: {e}")


def main(args):
    """Main function."""
    # Parse dates
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    dates = generate_date_range(start_date, end_date)
    
    print(f"Creating Zarr dataset for {len(dates)} dates")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Setup paths
    grib_dir = Path(args.grib_dir)
    output_path = Path(args.output)
    
    if not grib_dir.exists():
        raise FileNotFoundError(f"GRIB directory not found: {grib_dir}")
    
    # Find first available GRIB file to extract grid info
    first_grib = None
    for date in dates:
        grib_file = grib_dir / args.grib_pattern.format(date=date.strftime('%Y%m%d'))
        if grib_file.exists():
            first_grib = grib_file
            break
    
    if first_grib is None:
        raise FileNotFoundError("No GRIB files found in the specified date range")
    
    # Read grid information
    grid_info = read_grid_info(first_grib)
    print(f"Grid: {grid_info['nlat']} x {grid_info['nlon']} ({grid_info['grid_type']})")

    # Initialize Zarr store
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
    
    # Process each date
    for date_idx, date in enumerate(dates):
        grib_file = grib_dir / args.grib_pattern.format(date=date.strftime('%Y%m%d'))
        process_date(date, date_idx, grib_file, root)
    
    print(f"\nZarr dataset created successfully at {output_path}")
    print(f"Dataset info:")
    print(f"  Forecast variables: {', '.join(FORECAST_VARS)}")
    print(f"  Analysis variables: {', '.join(ANALYSIS_VARS)}")
    print(f"  Time dimension: {len(dates)}")
    print(f"  Forecast steps: {len(FORECAST_STEPS)} (0-48 hours)")
    print(f"  Spatial dimensions: {grid_info['nlat']} x {grid_info['nlon']}")


if __name__ == '__main__':
    today = datetime.now().strftime('%Y%m%d')
    
    parser = argparse.ArgumentParser(
        description='Create Zarr dataset from GRIB files'
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
        default='/leonardo_scratch/fast/DE374_lot2/extremes-dt_lumi',
        help='Directory containing GRIB files'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/leonardo_scratch/fast/DE374_lot2/zarr_deliv',
        help='Output Zarr dataset path'
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
    main(args)
