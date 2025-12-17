import os
import sys
import argparse
import datetime
import subprocess
from c_directories import c_directories
from c_api_request import c_api_request
import eccodes as ecc

def main(
    nwp:str,
    run_where:str,
    date_i:str,
    date_f:str,
):    
    dirs=c_directories(nwp,run_where)
    nwp_download=c_api_request(date_i,date_f, "70.5/-23.5/29.5/62.5")
    start = datetime.datetime.strptime(date_i, "%Y%m%d")
    end = datetime.datetime.strptime(date_f, "%Y%m%d")
    d = start
    if nwp=="ifs":
        while d <= end:
            ds = d.strftime("%Y%m%d")

            request = nwp_download.mars_get_ifs(ds)
            final_file = dirs.get_final_grib_path(ds)            
            nwp_download.perform_mars_request(request,final_file)

            d += datetime.timedelta(days=1)
    elif nwp=="edt":
        while d <= end:
            if d < datetime.datetime(2025, 2, 5):
                params = ["228","167", "168", "165", "166","228246", "228247"]
            else:
                params = ["228","167", "168", "165", "166", "131", "132"]

            ds = d.strftime("%Y%m%d")  
            final_file = dirs.get_final_grib_path(ds)                  
            for p in params:
                request = nwp_download.polytope_get_instant_variables_request(ds,p)
                tmp_grib_path = dirs.get_sfc_temp_path(ds, p)
                result=nwp_download.perform_politope_request(request, tmp_grib_path)
                if result:
                    # If "228" is in the output file path, modify step range to start from 0
                    if p=="228":                    
                        # Process and modify the GRIB messages
                        modified_messages = []
                        with open(tmp_grib_path, 'rb') as f:
                            while True:
                                msg = ecc.codes_grib_new_from_file(f)
                                if msg is None:
                                    break
                                
                                # # Get current step values
                                step_start = ecc.codes_get(msg, 'forecastTime')
                                step_end = ecc.codes_get(msg, 'endStep') if ecc.codes_is_defined(msg, 'endStep') else step_start
                                
                                # Set start step to 0
                                ecc.codes_set(msg, 'forecastTime', 0)
                                if ecc.codes_is_defined(msg, 'startStep'):
                                    ecc.codes_set(msg, 'startStep', 0)
                                    ecc.codes_set(msg, 'endStep', step_end)
                                
                                # Get the modified message as bytes
                                modified_messages.append(ecc.codes_get_message(msg))
                                ecc.codes_release(msg)
                        
                        # Write the modified messages back to the file
                        with open(tmp_grib_path, 'wb') as f:
                            for msg_bytes in modified_messages:
                                f.write(msg_bytes) 
                        print("Modifiche step completate per il file", tmp_grib_path)               

            edt_files = [f for f in os.listdir(dirs.nwp_temp) if "edt" in f and ds in f]

            if edt_files:
                final_file = dirs.get_final_grib_path(ds)
                edt_file_paths = [os.path.join(dirs.nwp_temp, f) for f in edt_files]
                
                subprocess.run(["grib_copy"] + edt_file_paths + [final_file], check=True)
                
                # Cleanup temporary files
                for file_path in edt_file_paths:
                    os.remove(file_path)

            d += datetime.timedelta(days=1)

    # if (date_f==datetime.date.today().strftime("%Y-%m-%d") and date_i==datetime.date.today().strftime("%Y-%m-%d")):
    #     print("Scaricamento giornata di previsione Extremes-DT per oggi", date_i)        
    #     # ----- Istantanee superficie (T2m, U10, V10, Td) -----
    #     instant_sfc_request = extremes_dt.polytope_get_instant_variables_request()
    #     instant_sfc_file = dirs.get_sfc_temp_path(date_i)
    #     extremes_dt.perform_politope_request(instant_sfc_request, instant_sfc_file)

    #     # ----- Precipitazione cumulata -----
    #     tp_request = extremes_dt.polytope_get_precip_request()
    #     tp_file = dirs.get_tp_temp_path(date_i)
    #     extremes_dt.perform_politope_request(tp_request, tp_file)


    #     final_file = dirs.get_final_grib_path(date_i)

    #     subprocess.run(["grib_copy", instant_sfc_file, tp_file, final_file],
    #                 check=True)

    #     # cleanup temporanei
    #     os.remove(instant_sfc_file)

    #     if tp_file:
    #         os.remove(tp_file)
    #     print("Download e merge completati per la data", date_i)
    # else: #download archived data from MARS
    #     df_available_date=pd.read_csv(dirs.extremesdt_available_dates_file, header=None, sep=" ")
    #     df_available_date=df_available_date.loc[(df_available_date[1]==336),0]
    #     start = datetime.datetime.strptime(date_i, "%Y-%m-%d")
    #     end = datetime.datetime.strptime(date_f, "%Y-%m-%d")
    #     d = start
    #     while d <= end:
    #         ds = d.strftime("%Y%m%d")
    #         if ds in df_available_date.values.astype(str):
    #             request = extremes_dt.mars_get_instant_variables_request(ds)
    #             final_file = dirs.get_final_grib_path(ds)            
    #             extremes_dt.perform_mars_request(request,final_file)
    #         else:
    #             print(f"Data {ds} non disponibile per Extremes-DT o incompleta (verificare {dirs.extremesdt_available_dates_file})")
    #         d += datetime.timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scarica previsioni di Extremes-DT")
    parser.add_argument(
        "--nwp",
        type=str,
        help="Modelli supportati: ifs, edt (default: ifs)",
        default="ifs",
    )     
    parser.add_argument(
        "--run_where",
        type=str,
        help="local, atos (ita3494), leonardo",
        default="local",
    )    
    parser.add_argument(
        "--date_i",
        type=str,
        help="Data in formato YYYYMMDD (default: oggi)",
        default=datetime.date.today().strftime("%Y%m%d"),
    )
    parser.add_argument(
        "--date_f",
        type=str,
        help="Data in formato YYYYMMDD (default: oggi)",
        default=datetime.date.today().strftime("%Y%m%d"),
    )

    args = parser.parse_args()

    # validazione formato data
    try:
        datetime.datetime.strptime(args.date_i, "%Y%m%d")
        datetime.datetime.strptime(args.date_f, "%Y%m%d")

    except ValueError:
        print("Errore: la data deve essere nel formato YYYYMMDD")
        sys.exit(1)

    main(args.nwp,args.run_where,args.date_i,args.date_f)
