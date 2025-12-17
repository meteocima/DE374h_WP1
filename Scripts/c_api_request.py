"""
c_api_request.py

API request handler for downloading weather forecast data from ECMWF sources.
Provides functionality to download data from both MARS (ECMWF IFS operational) 
and Polytope (Destination Earth Extremes-DT) services.

Supports downloading various meteorological parameters with flexible date ranges
and geographic bounding boxes.
"""

import datetime
from typing import Dict, Any, Optional
from pathlib import Path

import earthkit.data as ek


class c_api_request:
    """
    Handle API requests for weather forecast data from ECMWF services.
    
    This class provides methods to construct and execute requests for both
    MARS (Meteorological Archival and Retrieval System) and Polytope 
    (Destination Earth platform) data sources.
    """

    def __init__(self, date_i: str, date_f: str, area_bbox: str):
        """
        Initialize the API request handler.
        
        Args:
            date_i: Start date in YYYYMMDD format
            date_f: End date in YYYYMMDD format
            area_bbox: Geographic bounding box as "North/West/South/East"
        """
        self.date_i = date_i
        self.date_f = date_f
        self.polytope_address = "polytope.lumi.apps.dte.destination-earth.eu"
        self.area_bbox = area_bbox
    
    def perform_politope_request(
        self, 
        request_dict: Dict[str, Any], 
        output_file_path: Path
    ) -> bool:
        """
        Execute a Polytope request to download Extremes-DT data.
        
        Polytope is the Destination Earth platform API for accessing
        high-resolution weather and climate data.
        
        Args:
            request_dict: Dictionary containing the Polytope request parameters
            output_file_path: Path where the downloaded file should be saved
            
        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            # Connect to Polytope service and submit request
            data = ek.from_source(
                "polytope", 
                "ecmwf-destination-earth",
                request_dict,
                address=self.polytope_address,
                stream=False
            )
            
            print(f"[{datetime.datetime.now()}] Downloading {output_file_path}...")
            
            # Save data to specified file
            data.to_target("file", output_file_path)
            
            print(f"[{datetime.datetime.now()}] Saved {output_file_path}")
            return True
            
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Polytope request failed: {e}")
            return False         
        

    def polytope_get_instant_variables_request(
        self, 
        date: str, 
        param: str
    ) -> Dict[str, Any]:
        """
        Create a Polytope request for Extremes-DT instantaneous variables.
        
        Extremes-DT provides high-resolution (0.04°) weather forecasts with
        hourly temporal resolution. Different parameters require different
        level types and configurations.
        
        Args:
            date: Date in YYYYMMDD format
            param: ECMWF parameter code (e.g., '167' for 2m temperature)
            
        Returns:
            dict: Complete Polytope request dictionary
        """
        # Wind parameters at 100m height require special level configuration
        wind_100m_params = ["131", "132", "228246", "228247"]
        is_wind_100m = param in wind_100m_params
        
        # Precipitation requires different step configuration (accumulation periods)
        step_config = (
            "0-1/to/48/by/1" if param == "228" 
            else "0/to/48/by/1"
        )
        
        return {
            "class": "d1",                    # Destination Earth class 1
            "expver": "0001",                 # Experiment version
            "stream": "oper",                 # Operational stream
            "dataset": "extremes-dt",         # Extremes Digital Twin dataset
            "date": date,                     # Forecast base date
            "time": "0000",                   # 00 UTC base time
            "type": "fc",                     # Forecast type
            "levtype": "hl" if is_wind_100m else "sfc",  # Height/Surface level
            "levelist": "100" if is_wind_100m else "",   # 100m height
            "step": step_config,              # Forecast steps (hours)
            "param": param,                   # Meteorological parameter
            "area": self.area_bbox,           # Geographic bounds
            "grid": "0.04/0.04",             # ~4km resolution grid
        }

    def perform_mars_request(
        self, 
        request_dict: Dict[str, Any], 
        output_file_path: Path
    ) -> bool:
        """
        Execute a MARS request to download ECMWF operational data.
        
        MARS (Meteorological Archival and Retrieval System) provides access
        to ECMWF's operational weather forecast data including IFS model output.
        
        Args:
            request_dict: Dictionary containing the MARS request parameters
            output_file_path: Path where the downloaded file should be saved
            
        Returns:
            bool: True if download successful, False otherwise
        """
        print(f"[{datetime.datetime.now()}] Set request...")
        
        try:
            # Connect to MARS service and submit request
            data = ek.from_source("mars", request_dict)
            
            print(f"[{datetime.datetime.now()}] Downloading {output_file_path}...")
            
            # Save data to specified file
            data.to_target("file", output_file_path)
            
            print(f"[{datetime.datetime.now()}] Saved {output_file_path}")
            return True
            
        except Exception as e:
            print(f"[{datetime.datetime.now()}] MARS request failed: {e}")
            return False   
              
    def mars_get_ifs(self, date: str) -> Dict[str, Any]:
        """
        Create a MARS request for ECMWF IFS operational forecast data.
        
        Downloads a comprehensive set of surface meteorological parameters
        from the IFS (Integrated Forecasting System) operational model.
        Resolution is 0.1° (~10km) with 48-hour forecast range.
        
        Parameters included:
        - 167.128: 2m temperature
        - 168.128: 2m dewpoint temperature  
        - 165.128: 10m U wind component
        - 166.128: 10m V wind component
        - 228.128: Total precipitation
        - 228246/228247: 100m wind components
        - 39: Volumetric soil moisture (layer 1)
        - 40: Volumetric soil moisture (layer 2)
        - 41: Volumetric soil moisture (layer 3) 
        - 42: Volumetric soil moisture (layer 4)
        - 43: Soil type
        - 172: Land-sea mask
        - 26: Lake cover
        - 129: Geopotential (surface orography)
        
        Args:
            date: Forecast base date in YYYYMMDD format
            
        Returns:
            dict: Complete MARS request dictionary
        """
        # Generate forecast steps from 0 to 48 hours
        forecast_steps = "/".join(str(i) for i in range(0, 49))
        
        return {
            "class": "od",                   # Operational data
            "expver": "1",                   # Latest operational version
            "stream": "oper",                # Operational stream
            "type": "fc",                    # Forecast type
            "levtype": "sfc",               # Surface level type
            "param": (                       # Multiple parameters in single request
                "167.128/168.128/165.128/166.128/228.128/"  # Basic met variables
                "228246/228247/39/40/41/42/43/172/26/129"    # Wind, soil, masks
            ),
            "date": date,                    # Forecast base date
            "time": "00:00:00",             # 00 UTC base time
            "step": forecast_steps,          # All forecast hours 0-48
            "grid": "0.1/0.1",              # 0.1° resolution (~10km)
            "area": self.area_bbox,          # Geographic bounds
        }     

    # Legacy method - kept for reference
    # def mars_get_instant_variables_request(self, date: str) -> Dict[str, Any]:
    #     """
    #     Legacy MARS request method for research data (commented out).
    #     
    #     This method was used for accessing research data with higher resolution
    #     but is no longer actively used in favor of the operational IFS data.
    #     
    #     Args:
    #         date: Forecast base date
    #         
    #     Returns:
    #         dict: MARS request dictionary for research data
    #     """
    #     return {
    #         "class": "rd",                   # Research data
    #         "expver": "iekm",               # Experiment version
    #         "stream": "oper",               # Operational stream
    #         "type": "fc",                   # Forecast type
    #         "levtype": "sfc",              # Surface level
    #         "param": "167/168/165/166/228/228246/228247",  # Met parameters
    #         "date": date,                   # Base date
    #         "time": "0000",                # Base time
    #         "step": "/".join(str(i) for i in range(1, 49)),  # Steps 1-48
    #         "grid": "0.04/0.04",           # 4km resolution
    #         "area": "70.5/-23.5/29.5/62.5",  # Europe region
    #         "format": "grib2",             # GRIB2 format
    #     }    
    
