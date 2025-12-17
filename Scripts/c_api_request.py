import earthkit.data as ek
import datetime

class c_api_request():
    def __init__(self, date_i, date_f, area_bbox):
        self.date_i=date_i
        self.date_f=date_f
        self.polytope_address="polytope.lumi.apps.dte.destination-earth.eu"
        self.area_bbox=area_bbox
    
    def perform_politope_request(self, request_dict, output_file_path):
        try:
            data = ek.from_source("polytope", "ecmwf-destination-earth",
                                   request_dict,
                                   address=self.polytope_address,
                                   stream=False)
            print(f"[{datetime.datetime.now()}] Downloading {output_file_path}...")
            data.to_target("file", output_file_path)
            print(f"[{datetime.datetime.now()}] Saved {output_file_path}")
            return True
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Polytope request failed: {e}")
            return False         
        

    def polytope_get_instant_variables_request(self, date, param):
        return {
                "class": "d1",
                "expver": "0001",
                "stream": "oper",
                "dataset": "extremes-dt",
                "date": date,
                "time": "0000",
                "type": "fc",
                "levtype": "hl" if param in ["131","132","228246", "228247"] else "sfc",
                "levelist": "100" if param in ["131","132","228246", "228247"] else "",
                "step": "0/to/48/by/1" if param != "228" else "0-1/to/48/by/1",
                "param": param, #"167/165/166/168",  # T2m, U10, V10, Td
                "area": self.area_bbox,
                "grid": "0.04/0.04",
            }

    # def polytope_get_precip_request(self, date):
    #     return {
    #                 "class": "d1",
    #                 "expver": "0001",
    #                 "stream": "oper",
    #                 "dataset": "extremes-dt",
    #                 "date": date,
    #                 "time": "0000",
    #                 "type": "fc",
    #                 "levtype": "sfc",
    #                 "step": "0-1/to/3/by/1",
    #                 "param": "228",  # TP
    #                 "area": self.area_bbox,
    #                 "grid": "0.04/0.04",
    #            }

    def perform_mars_request(self, request_dict, output_file_path):
        print(f"[{datetime.datetime.now()}] Set request...")
        try:
            data = ek.from_source("mars", request_dict)
            print(f"[{datetime.datetime.now()}] Downloading {output_file_path}...")
            data.to_target("file", output_file_path)
            print(f"[{datetime.datetime.now()}] Saved {output_file_path}")
            return True
        except Exception as e:
            print(f"[{datetime.datetime.now()}] MARS request failed: {e}")
            return False   
              
    def mars_get_ifs(self, date):
        return {
            "class": "od",
            "expver": "1",
            "stream": "oper",
            "type": "fc",
            "levtype": "sfc",
            "param": "167.128/168.128/165.128/166.128/228.128/228246/228247/39/40/41/42/43/172/26/129",
            "date": date,                    
            "time": "00:00:00",                    
            "step": "/".join(str(i) for i in range(0, 49)),
            "grid": "0.1/0.1",
            "area": self.area_bbox,
        }     
    # def mars_get_instant_variables_request(self, date):
    #     return {
    #         "class": "rd",
    #         "expver": "iekm",
    #         "stream": "oper",
    #         "type": "fc",
    #         "levtype": "sfc",
    #         "param": "167/168/165/166/228/228246/228247",#"167/168/165/166/228",  # t2m, td, u10, v10, tp
    #         "date": date,                    # YYYY-MM-DD
    #         "time": "0000",                    # HH
    #         "step": "/".join(str(i) for i in range(1, 49)),
    #         "grid": "0.04/0.04",             # ~4 km
    #         "area": "70.5/-23.5/29.5/62.5",
    #         "format": "grib2",
    #     }    
    
