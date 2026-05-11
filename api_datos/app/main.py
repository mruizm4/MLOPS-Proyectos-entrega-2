# ============================================================
# 📦 IMPORTS
# ============================================================

import zipfile
import requests

from io import BytesIO

import pandas as pd
import numpy as np

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

# ============================================================
# CONFIG
# ============================================================

DATASET_ZIP_URL = (
    "https://archive.ics.uci.edu/static/public/"
    "296/diabetes+130-us+hospitals+for+years+1999-2008.zip"
)

BATCH_SIZE = 1000

ID_MAPPING_COLUMNS = [
    "admission_type_id",
    "discharge_disposition_id",
    "admission_source_id"
]

# ============================================================
# APP
# ============================================================

app = FastAPI(
    title="Diabetes Dataset Batch API",
    version="4.0.0"
)

df = None
ids_mapping_df = None

# ============================================================
# DOWNLOAD ZIP
# ============================================================

def download_dataset_zip():
    """
    Downloads official UCI dataset ZIP.
    """

    print("⬇️ Downloading dataset ZIP...")

    response = requests.get(
        DATASET_ZIP_URL,
        timeout=120
    )

    response.raise_for_status()

    print("✅ ZIP downloaded")

    return BytesIO(response.content)

# ============================================================
# LOAD DATASET
# ============================================================

def load_dataset(zip_bytes):
    """
    Loads diabetic_data.csv from ZIP.
    """

    print("⬇️ Loading dataset...")

    with zipfile.ZipFile(zip_bytes) as z:

        with z.open("diabetic_data.csv") as f:

            df = pd.read_csv(
                f,
                low_memory=False
            )

    print(
        f"✅ Dataset loaded: "
        f"{len(df)} rows"
    )

    return df

# ============================================================
# LOAD IDS MAPPING
# ============================================================

def load_ids_mapping(zip_bytes):
    """
    Loads IDS_mapping.csv which contains
    multiple CSV tables concatenated together.
    """

    print("⬇️ Loading IDs mapping...")

    records = []

    with zipfile.ZipFile(zip_bytes) as zip_ref:

        with zip_ref.open("IDS_mapping.csv") as f:

            lines = (
                f.read()
                .decode("utf-8")
                .splitlines()
            )

    current_field = None

    for line in lines:

        line = line.strip()

        # ----------------------------------------
        # EMPTY / SEPARATOR ROW
        # ----------------------------------------

        if (
            not line
            or line == ","
        ):
            continue

        # ----------------------------------------
        # SPLIT CSV
        # ----------------------------------------

        parts = line.split(",", 1)

        if len(parts) < 2:
            continue

        left = parts[0].strip()
        right = parts[1].strip()

        # ----------------------------------------
        # HEADER ROW
        # Example:
        # admission_type_id,description
        # ----------------------------------------

        if right.lower() == "description":

            current_field = left

            print(f"📌 Current field: {current_field}")

            continue

        # ----------------------------------------
        # DATA ROW
        # ----------------------------------------

        if current_field:

            try:

                mapping_id = int(left)

                description = right.strip('"')

                records.append(
                    {
                        "field": current_field,
                        "id": mapping_id,
                        "description": description
                    }
                )

            except Exception:
                continue

    mapping_df = pd.DataFrame(records)

    print(
        f"✅ IDs mapping loaded: "
        f"{len(mapping_df)} rows"
    )

    print(mapping_df.head())

    return mapping_df

# ============================================================
# APPLY IDS MAPPING
# ============================================================

def apply_id_mappings(
    dataframe: pd.DataFrame,
    mapping_df: pd.DataFrame
):
    """
    Replaces ID columns with categorical labels.
    """

    mapped_df = dataframe.copy()

    mapping_columns = [
        "admission_type_id",
        "discharge_disposition_id",
        "admission_source_id"
    ]

    for column in mapping_columns:

        if column not in mapped_df.columns:
            continue

        # ----------------------------------------
        # BUILD DICTIONARY
        # ----------------------------------------

        subset = mapping_df[
            mapping_df["field"] == column
        ]

        mapping_dict = dict(
            zip(
                subset["id"],
                subset["description"]
            )
        )

        # ----------------------------------------
        # APPLY MAPPING
        # ----------------------------------------

        mapped_df[column] = (
            pd.to_numeric(
                mapped_df[column],
                errors="coerce"
            )
            .map(mapping_dict)
            .fillna("Unknown")
        )

        #print(mapped_df)

    return mapped_df
# ============================================================
# JSON SAFE
# ============================================================

def dataframe_to_json_safe(
    dataframe
):
    """
    Converts dataframe into JSON-safe format.
    """

    safe_df = dataframe.copy()

    safe_df = safe_df.replace(
        [np.inf, -np.inf],
        np.nan
    )

    safe_df = safe_df.astype(object)

    safe_df = safe_df.where(
        pd.notnull(safe_df),
        None
    )

    return safe_df.to_dict(
        orient="records"
    )

# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
def startup_event():

    global df
    global ids_mapping_df

    print("🚀 Starting API...")

    zip_bytes = download_dataset_zip()

    ids_mapping_df = load_ids_mapping(
        zip_bytes
    )

    # IMPORTANT:
    # rewind BytesIO after reading ZIP
    zip_bytes.seek(0)

    raw_df = load_dataset(
        zip_bytes
    )

    df = apply_id_mappings(
        raw_df,
        ids_mapping_df
    )

    print("✅ API ready")

# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "ok",
        "rows_loaded": len(df),
        "mapping_rows": len(ids_mapping_df),
        "mapped_columns": ID_MAPPING_COLUMNS
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
            detail="Dataset not loaded"
        )

    sample_size = min(
        BATCH_SIZE,
        len(df)
    )

    sampled_df = df.sample(
        n=sample_size,
        replace=False
    )

    data = dataframe_to_json_safe(
        sampled_df
    )

    return JSONResponse(
        content=jsonable_encoder(
            {
                "batch_size": sample_size,
                "mapped_columns": ID_MAPPING_COLUMNS,
                "data": data
            }
        )
    )

# ============================================================
# RANDOM BATCH WITH SEED
# ============================================================

@app.get("/batch/{seed}")
def get_random_batch_seed(
    seed: int
):

    global df

    if df is None:

        raise HTTPException(
            status_code=500,
            detail="Dataset not loaded"
        )

    sample_size = min(
        BATCH_SIZE,
        len(df)
    )

    sampled_df = df.sample(
        n=sample_size,
        replace=False,
        random_state=seed
    )

    data = dataframe_to_json_safe(
        sampled_df
    )

    return JSONResponse(
        content=jsonable_encoder(
            {
                "seed": seed,
                "batch_size": sample_size,
                "mapped_columns": ID_MAPPING_COLUMNS,
                "data": data
            }
        )
    )

# ============================================================
# IDS MAPPING
# ============================================================

@app.get("/ids_mapping")
def get_ids_mapping():

    global ids_mapping_df

    if ids_mapping_df is None:

        raise HTTPException(
            status_code=500,
            detail="IDs mapping not loaded"
        )

    data = dataframe_to_json_safe(
        ids_mapping_df
    )

    return JSONResponse(
        content=jsonable_encoder(
            {
                "rows": len(data),
                "data": data
            }
        )
    )

# ============================================================
# FEATURE CATEGORIES
# ============================================================

@app.get("/feature_categories")
def get_feature_categories():
    """
    Returns categorical values grouped by field.
    """

    global ids_mapping_df

    grouped = {}

    for field in (
        ids_mapping_df["field"]
        .unique()
    ):

        values = ids_mapping_df[
            ids_mapping_df["field"] == field
        ]["description"].tolist()

        grouped[field] = values

    return grouped