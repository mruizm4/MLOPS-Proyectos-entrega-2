# app/main.py

import os
import zipfile
import random
import pandas as pd
import requests

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import numpy as np

# ============================================================
# CONFIG
# ============================================================

DATA_URL = "https://archive.ics.uci.edu/static/public/296/diabetes+130-us+hospitals+for+years+1999-2008.zip"

DATA_DIR = "data"
ZIP_PATH = os.path.join(DATA_DIR, "dataset.zip")
CSV_PATH = os.path.join(DATA_DIR, "diabetic_data.csv")

BATCH_SIZE = 15

# ============================================================
# APP
# ============================================================

app = FastAPI(
    title="Diabetes Dataset Batch API",
    version="1.0.0"
)

df = None


# ============================================================
# DOWNLOAD DATASET
# ============================================================

def download_dataset():

    os.makedirs(DATA_DIR, exist_ok=True)

    # Si ya existe, no descargar de nuevo
    if os.path.exists(CSV_PATH):
        print("✅ Dataset ya existe")
        return

    print("⬇️ Descargando dataset...")

    response = requests.get(DATA_URL, timeout=120)
    response.raise_for_status()

    with open(ZIP_PATH, "wb") as f:
        f.write(response.content)

    print("📦 Extrayendo dataset...")

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        zip_ref.extractall(DATA_DIR)

    print("✅ Dataset listo")


# ============================================================
# STARTUP EVENT
# ============================================================

@app.on_event("startup")
def startup_event():

    global df

    download_dataset()

    print("📖 Cargando CSV en memoria...")

    df = pd.read_csv(CSV_PATH)

    print(f"✅ Dataset cargado: {len(df)} registros")


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "rows_loaded": len(df)
    }


# ============================================================
# RANDOM BATCH
# ============================================================



@app.get("/batch")
def get_random_batch():

    global df

    if df is None:
        raise HTTPException(
            status_code=500,
            detail="Dataset no cargado"
        )

    sample_size = min(BATCH_SIZE, len(df))

    sampled_df = df.sample(
        n=sample_size,
        replace=False
    ).copy()

    # =========================================================
    # LIMPIEZA JSON SAFE
    # =========================================================

    sampled_df = sampled_df.replace(
        [np.inf, -np.inf],
        np.nan
    )

    sampled_df = sampled_df.astype(object)

    sampled_df = sampled_df.where(
        pd.notnull(sampled_df),
        None
    )

    data = sampled_df.to_dict(orient="records")

    return JSONResponse(
        content=jsonable_encoder({
            "batch_size": sample_size,
            "data": data
        })
    )


# ============================================================
# RANDOM BATCH WITH SEED
# ============================================================

@app.get("/batch/{seed}")
def get_random_batch_seed(seed: int):

    global df

    if df is None:
        raise HTTPException(
            status_code=500,
            detail="Dataset no cargado"
        )

    sample_size = min(BATCH_SIZE, len(df))

    sampled_df = df.sample(
        n=sample_size,
        replace=False,
        random_state=seed
    ).copy()

    sampled_df = sampled_df.replace(
        [np.inf, -np.inf],
        np.nan
    )

    sampled_df = sampled_df.astype(object)

    sampled_df = sampled_df.where(
        pd.notnull(sampled_df),
        None
    )

    data = sampled_df.to_dict(orient="records")

    return JSONResponse(
        content=jsonable_encoder({
            "seed": seed,
            "batch_size": sample_size,
            "data": data
        })
    )