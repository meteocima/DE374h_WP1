"""
c_directories.py

Directory management system for weather forecast data download and storage.
Handles path configuration for different computing environments (local, Leonardo HPC)
and different NWP models (IFS, Extremes-DT).

Provides organized directory structure for:
- Data storage (final GRIB files)
- Temporary processing files
- Log files and monitoring
- Script locations
"""

from pathlib import Path

class c_directories:
    """
    Directory structure manager for weather forecast data processing.
    
    Configures appropriate directory paths based on the computing environment
    and NWP model type. Handles different storage requirements for local
    development versus HPC cluster environments.
    
    Attributes:
        run_where: Computing environment ('local' or 'leonardo')
        nwp: NWP model type ('ifs' or 'edt')
        root_path: Base project directory
        data_path: Final data storage location
        scratch_path: Temporary processing directory
        nwp_temp: Temporary file storage
        nwp_log: Log file directory (HPC only)
        nwp_check: Data validation directory (HPC only)
        Scripts_path: Script file location
    """

    def __init__(self, nwp: str, run_where: str):
        """
        Initialize directory structure based on environment and NWP model.
        
        Args:
            nwp: NWP model type ('ifs' for ECMWF IFS, 'edt' for Extremes-DT)
            run_where: Computing environment ('local' or 'leonardo')
        """
        self.run_where = run_where
        self.nwp = nwp

        # Get user home directory as base reference
        home = Path.home()
        # Configure paths for local development environment
        if run_where == "local":
            # Simple flat structure for local development
            self.root_path = home / "CIMA" / "202508_ECMWF_LOT2_DATAPROCUREMENT"
            self.data_path = self.root_path
            self.scratch_path = self.root_path
            self.nwp_temp = self.root_path 


        # Configure paths for Leonardo HPC cluster environment
        elif run_where == "leonardo":
            # Leonardo HPC has separate scratch and work storage areas
            leo_scratch = Path("/leonardo_scratch/")  # Fast, temporary storage
            leo_storage = Path("/leonardo_work/")     # Long-term storage
            
            self.root_path = home / "DE374_lot2"
            
            # Different storage strategies based on NWP model
            if self.nwp == "ifs":
                # IFS: Final data on work storage, processing on scratch
                self.data_path = leo_storage / "DE374_lot2_0" / "IFS"
                self.scratch_path = (
                    leo_scratch / "large" / "userexternal" / "lmonaco0" / 
                    "DE374_lot2" / "ifs"
                )
            else:
                # EDT: Both data and processing on scratch (faster access)
                self.data_path = leo_scratch / "fast" / "DE374_lot2" / "extremes-dt"
                self.scratch_path = (
                    leo_scratch / "large" / "userexternal" / "lmonaco0" / 
                    "DE374_lot2" / "extremes-dt"
                )
            
            # Create organized subdirectories for HPC workflow
            self.nwp_temp = self.scratch_path / "temp"     # Temporary files
            self.nwp_log = self.scratch_path / "log"       # Job logs
            self.nwp_check = self.scratch_path / "check"   # Data validation

        # Common script directory for all environments
        self.Scripts_path = self.root_path / "Scripts"


    def get_sfc_temp_path(self, date: str, param: str) -> Path:
        """
        Generate path for temporary surface parameter GRIB file.
        
        Used during EDT processing where each parameter is downloaded
        separately before merging into daily files.
        
        Args:
            date: Date in YYYYMMDD format
            param: ECMWF parameter code (e.g., '167', '228')
            
        Returns:
            Path: Complete path to temporary parameter file
        """
        return self.nwp_temp / f"{self.nwp}_{param}_{date}.grib"
    
    def get_tp_temp_path(self, date: str) -> Path:
        """
        Generate path for temporary precipitation GRIB file.
        
        Legacy method for separate precipitation file handling.
        Currently used in commented-out precipitation processing code.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Path: Complete path to temporary precipitation file
        """
        return self.nwp_temp / f"{self.nwp}_tp_{date}.grib"
    
    def get_final_grib_path(self, date: str) -> Path:
        """
        Generate path for final daily GRIB file.
        
        This is the final merged file containing all parameters for a
        single day, stored in the appropriate data directory.
        
        Args:
            date: Date in YYYYMMDD format
            
        Returns:
            Path: Complete path to final daily GRIB file
        """
        return self.data_path / f"{self.nwp}_{date}.grib"  