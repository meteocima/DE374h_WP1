"""
DE374h_download.py

Download script for weather forecast data from ECMWF IFS and Extremes-DT.
Supports multiple data sources (IFS operational, Extremes-DT) and different
computing environments (local, ATOS, Leonardo).

Usage:
    python DE374h_download.py --nwp ifs --run_where leonardo --date_i 20251215 --date_f 20251217
"""

# Standard library imports
import os
import sys
import argparse
import datetime
import subprocess

# Local imports
from c_directories import c_directories
from c_api_request import c_api_request

# Third-party imports
import eccodes as ecc

def main(
    nwp: str,
    run_where: str,
    date_i: str,
    date_f: str,
):
    """
    Main function to download weather forecast data.
    
    Args:
        nwp: NWP model type ('ifs' or 'edt')
        run_where: Computing environment ('local', 'atos', or 'leonardo')
        date_i: Start date in YYYYMMDD format
        date_f: End date in YYYYMMDD format
    """
    # Initialize directory structure based on NWP model and environment
    dirs = c_directories(nwp, run_where)
    
    # Initialize API request handler with date range and geographic bounds
    # Area: North/West/South/East = 70.5/-23.5/29.5/62.5 (Europe region)
    nwp_download = c_api_request(date_i, date_f, "70.5/-23.5/29.5/62.5")
    
    # Convert string dates to datetime objects for iteration
    start = datetime.datetime.strptime(date_i, "%Y%m%d")
    end = datetime.datetime.strptime(date_f, "%Y%m%d")
    # Initialize date iterator
    d = start
    
    # Process IFS operational data via MARS
    if nwp == "ifs":
        while d <= end:
            # Convert datetime to string format for file naming
            ds = d.strftime("%Y%m%d")

            # Create MARS request for IFS data
            request = nwp_download.mars_get_ifs(ds)
            final_file = dirs.get_final_grib_path(ds)
            
            # Execute MARS request and download to final file
            nwp_download.perform_mars_request(request, final_file)

            # Move to next day
            d += datetime.timedelta(days=1)
    # Process Extremes-DT data via Polytope
    elif nwp == "edt":
        while d <= end:
            # Parameter list changes based on date due to system updates
            if d < datetime.datetime(2025, 2, 5):
                # Old parameter set: includes 228246, 228247 (100m wind components)
                params = ["228", "167", "168", "165", "166", "228246", "228247"]
            else:
                # New parameter set: includes 131, 132 (u/v wind components)
                params = ["228", "167", "168", "165", "166", "131", "132"]

            ds = d.strftime("%Y%m%d")
            final_file = dirs.get_final_grib_path(ds)
            # Process each weather parameter separately
            for p in params:
                # Create Polytope request for specific parameter
                request = nwp_download.polytope_get_instant_variables_request(ds, p)
                tmp_grib_path = dirs.get_sfc_temp_path(ds, p)
                
                # Download parameter data to temporary file
                result = nwp_download.perform_politope_request(request, tmp_grib_path)
                
                if result:
                    # Special handling for precipitation (param 228)
                    # Need to modify GRIB step metadata to start from 0
                    if p == "228":                    
                        # Process and modify the GRIB messages for precipitation
                        # Precipitation accumulation needs step adjustment
                        modified_messages = []
                        with open(tmp_grib_path, 'rb') as f:
                            while True:
                                # Read next GRIB message from file
                                msg = ecc.codes_grib_new_from_file(f)
                                if msg is None:
                                    break
                                
                                # Extract current forecast step information
                                step_start = ecc.codes_get(msg, 'forecastTime')
                                step_end = ecc.codes_get(msg, 'endStep') if ecc.codes_is_defined(msg, 'endStep') else step_start
                                
                                # Modify step to start from 0 (for accumulation)
                                ecc.codes_set(msg, 'forecastTime', 0)
                                if ecc.codes_is_defined(msg, 'startStep'):
                                    ecc.codes_set(msg, 'startStep', 0)
                                    ecc.codes_set(msg, 'endStep', step_end)
                                
                                # Store modified message for writing back
                                modified_messages.append(ecc.codes_get_message(msg))
                                ecc.codes_release(msg)
                        
                        # Write the modified messages back to the file
                        with open(tmp_grib_path, 'wb') as f:
                            for msg_bytes in modified_messages:
                                f.write(msg_bytes)
                        print("Modifiche step completate per il file", tmp_grib_path)               

            # Find all EDT temporary files for current date
            edt_files = [
                f for f in os.listdir(dirs.nwp_temp)
                if "edt" in f and ds in f
            ]

            # Merge all parameter files into single daily file
            if edt_files:
                final_file = dirs.get_final_grib_path(ds)
                edt_file_paths = [
                    os.path.join(dirs.nwp_temp, f) for f in edt_files
                ]
                
                # Use grib_copy to concatenate all parameter files
                subprocess.run(
                    ["grib_copy"] + edt_file_paths + [final_file],
                    check=True
                )
                
                # Remove temporary files to save disk space
                for file_path in edt_file_paths:
                    os.remove(file_path)

            # Move to next day
            d += datetime.timedelta(days=1)


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description="Download weather forecast data from ECMWF IFS or Extremes-DT"
    )
    
    # NWP model selection
    parser.add_argument(
        "--nwp",
        type=str,
        help="NWP model: 'ifs' (ECMWF operational) or 'edt' (Extremes-DT)",
        default="ifs",
    )
    
    # Computing environment selection
    parser.add_argument(
        "--run_where",
        type=str,
        help="Computing environment: 'local', 'atos' (ita3494), or 'leonardo'",
        default="local",
    )    
    # Date range arguments
    parser.add_argument(
        "--date_i",
        type=str,
        help="Start date in YYYYMMDD format (default: today)",
        default=datetime.date.today().strftime("%Y%m%d"),
    )
    parser.add_argument(
        "--date_f",
        type=str,
        help="End date in YYYYMMDD format (default: today)",
        default=datetime.date.today().strftime("%Y%m%d"),
    )

    # Parse command line arguments
    args = parser.parse_args()

    # Validate date format before proceeding
    try:
        datetime.datetime.strptime(args.date_i, "%Y%m%d")
        datetime.datetime.strptime(args.date_f, "%Y%m%d")
    except ValueError:
        print("Error: Date must be in YYYYMMDD format")
        sys.exit(1)

    # Execute main function with parsed arguments
    main(args.nwp, args.run_where, args.date_i, args.date_f)
