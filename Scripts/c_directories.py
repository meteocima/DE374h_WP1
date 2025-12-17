from pathlib import Path

class c_directories():
    def __init__(self, nwp, run_where):
        self.run_where=run_where
        self.nwp=nwp

        home = Path.home()
        if run_where=="local":
            self.root_path=home / "CIMA" / "202508_ECMWF_LOT2_DATAPROCUREMENT"
            self.data_path=self.root_path
            self.scratch_path=self.root_path
            self.nwp_temp = self.root_path 


        elif run_where=="leonardo":
            leo_scratch=Path("/leonardo_scratch/")
            leo_storage=Path("/leonardo_work/")
            self.root_path=home / "DE374_lot2"
            if self.nwp=="ifs":
                self.data_path = leo_storage / "DE374_lot2_0" / "IFS" 
                self.scratch_path = leo_scratch / "large" / "userexternal" / "lmonaco0" / "DE374_lot2" / "ifs"
            else:
                self.data_path=leo_scratch / "fast" / "DE374_lot2" / "extremes-dt"
                self.scratch_path=leo_scratch / "large" / "userexternal" / "lmonaco0" / "DE374_lot2" / "extremes-dt"
            self.nwp_temp = self.scratch_path / "temp"
            self.nwp_log = self.scratch_path / "log"
            self.nwp_check = self.scratch_path / "check"

        self.Scripts_path = self.root_path / "Scripts"


    def get_sfc_temp_path(self, date, param):
        return self.nwp_temp / f"{self.nwp}_{param}_{date}.grib"
    def get_tp_temp_path(self, date):
        return self.nwp_temp / f"{self.nwp}_tp_{date}.grib"    
    def get_final_grib_path(self, date):
        return self.data_path / f"{self.nwp}_{date}.grib"  