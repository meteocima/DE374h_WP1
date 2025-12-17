import zarr
import numpy as np
import pygrib
from numcodecs import Blosc


# ============================================================
#  COMPRESSORE
# ============================================================

# Zstd + Blosc: ottimo per GRIB meteorologici
compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)


# ============================================================
#  FUNCTIONS
# ============================================================

def load_field_from_grib(model, date, timestep, field):
    """
    Carica da GRIB il campo richiesto per un dato lead time (timestep).
    Adatta il filtro del messaggio GRIB alle tue convenzioni.
    """
    grib_path = f"/path/{model}/{date:%Y%m%d}.grib"
    with pygrib.open(grib_path) as grbs:
        # Cambia 'stepRange' secondo la struttura dei tuoi GRIB
        msg = grbs.select(shortName=field, stepRange=str(timestep))[0]
        return msg.values.astype("float32")


def get_native_shape(model, sample_date, sample_timestep, sample_field):
    """
    Restituisce (ny, nx) della griglia nativa del modello.
    """
    grib_path = f"/path/{model}/{sample_date:%Y%m%d}.grib"
    with pygrib.open(grib_path) as grbs:
        msg = grbs.select(shortName=sample_field, stepRange=str(sample_timestep))[0]
        arr = msg.values
        return arr.shape  # (ny, nx)


def initialize_zarr(root_path, dates, timesteps, models, fields):
    """
    Crea la struttura Zarr.
    Un gruppo per modello, ciascuno con griglia nativa diversa.
    """
    root = zarr.open(root_path, mode="w")

    # Coordinate comuni
    root.create_dataset("date",     data=np.array([d.isoformat() for d in dates], dtype=object))
    root.create_dataset("field",    data=np.array(fields, dtype=object))
    root.create_dataset("timestep", data=np.array(timesteps, dtype="int32"))
    root.create_dataset("model",    data=np.array(models, dtype=object))

    # Gruppi per ogni modello
    for model in models:
        ny, nx = get_native_shape(model, dates[0], timesteps[0], fields[0])
        grp = root.create_group(model)

        grp.create(
            "data",
            shape=(len(dates), len(timesteps), len(fields), ny, nx),
            chunks=(1, 1, 1, ny, nx),
            dtype="float32",
            compressor=compressor,
        )

    return root


def write_one_date(root, idt, date, timesteps, models, fields):
    """
    Scrive tutti i timesteps e campi di una data per ogni modello.
    """
    for model in models:
        arr = root[model]["data"]

        for its, ts in enumerate(timesteps):
            for ifl, field in enumerate(fields):

                data = load_field_from_grib(model, date, ts, field)
                arr[idt, its, ifl, :, :] = data


# ============================================================
#  MAIN EXECUTION
# ============================================================

def build_dataset(root_path, dates, timesteps, models, fields):
    root = initialize_zarr(root_path, dates, timesteps, models, fields)

    for idt, date in enumerate(dates):
        write_one_date(root, idt, date, timesteps, models, fields)


# ============================================================
#  USO
# ============================================================

dates     = [...]               # lista datetime
timesteps = list(range(1, 49))  # 1h–48h
models    = ["IFS", "EXTREMES-DT", "ICONEU"]
fields    = ["t2m", "u10", "v10","td2m","rh2m","u100","v100"]

build_dataset("dataset.zarr", dates, timesteps, models, fields)
