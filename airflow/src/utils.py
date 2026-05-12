import os
import json
import warnings
import pandas as pd
import numpy as np
import requests
import sqlalchemy as sa

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import text

from catboost import CatBoostClassifier

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score
)
import tempfile
import joblib
import mlflow
import mlflow.catboost
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature

from minio import Minio
import os


from sklearn.model_selection import train_test_split

from datetime import datetime
from typing import List, Dict, Optional

# MinIO / S3
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"

# Credenciales MinIO
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "supersecret"

# Opcional pero recomendado para MinIO
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


MYSQL_HOST = "mysql_db"
MYSQL_PORT = 3306
MYSQL_DB = "mlops_db"
MYSQL_USER = "mlops_user"
MYSQL_PASSWORD = "mlops_pass"

MLFLOW_TRACKING_URI = "http://mlflow:5000"
MLFLOW_EXPERIMENT = "diabetes_readmission"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "supersecret"

MINIO_BUCKET = "mlflow-artifacts"

CLEAN_TABLE = "dataset_split"

TARGET_COLUMN = "target"

DROP_COLUMNS = [
    "readmitted",
    TARGET_COLUMN
]


# ============================================================
# ⚙️ CONFIGURACIÓN GENERAL
# ============================================================

# -----------------------------
# API CONFIG
# -----------------------------
API_BASE_URL = "http://localhost:8003"

HEALTH_ENDPOINT = f"{API_BASE_URL}/health"
BATCH_ENDPOINT = f"{API_BASE_URL}/batch"

# -----------------------------
# DATABASE CONFIG
# -----------------------------


DATABASE_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# -----------------------------
# PIPELINE CONFIG
# -----------------------------
BATCH_SIZE = 1000

RAW_TABLE = "raw_diabetes_data"
CLEAN_TABLE = "clean_diabetes_data"
SPLIT_TABLE = "dataset_split"

# porcentaje splits
TRAIN_SIZE = 0.7
VAL_SIZE = 0.15
TEST_SIZE = 0.15


# ============================================================
# 🔌 CREAR CONEXIÓN SQLALCHEMY
# ============================================================

def create_engine_connection() -> Engine:
    """
    Crea y retorna una conexión SQLAlchemy hacia MySQL.

    Returns
    -------
    Engine
        Engine de SQLAlchemy conectado a MySQL.
    """

    engine = sa.create_engine(
        DATABASE_URL,
        pool_pre_ping=True
    )

    return engine



# ============================================================
# 🩺 VALIDAR DISPONIBILIDAD API
# ============================================================

def validate_api_health() -> bool:
    """
    Verifica que la API se encuentre disponible.

    Returns
    -------
    bool
        True si la API responde correctamente.
    """

    response = requests.get(HEALTH_ENDPOINT)

    if response.status_code != 200:
        raise Exception(
            f"API no disponible. Status code: {response.status_code}"
        )

    print("✅ API disponible")

    return True


# ============================================================
# 📥 OBTENER BATCH DE DATOS
# ============================================================

def fetch_batch(batch_size: int = 1000) -> pd.DataFrame:
    """
    Consume el endpoint batch de la API y retorna un DataFrame.

    Parameters
    ----------
    batch_size : int
        Cantidad de registros solicitados.

    Returns
    -------
    pd.DataFrame
        Datos obtenidos desde la API.
    """

    params = {
        "batch_size": batch_size
    }

    response = requests.get(
        BATCH_ENDPOINT,
        params=params
    )

    if response.status_code != 200:
        raise Exception(
            f"Error consumiendo batch API: {response.text}"
        )

    payload = response.json()

    data = payload["data"]

    df = pd.DataFrame(data)

    print(f"✅ Batch obtenido: {len(df)} registros")

    return df


# ============================================================
# 🧹 NORMALIZAR MISSING VALUES
# ============================================================

def normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reemplaza los valores '?' por None/NaN.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame original.

    Returns
    -------
    pd.DataFrame
        DataFrame limpio.
    """

    df = df.replace("?", "missing")
    df = df.fillna("missing")

    return df


# ============================================================
# 🔤 NORMALIZAR NOMBRES COLUMNAS
# ============================================================

def normalize_column_names(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Normaliza nombres de columnas para SQL.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    df = df.copy()

    df.columns = [
        col.replace("-", "_")
        .replace(" ", "_")
        .lower()
        for col in df.columns
    ]

    return df

# ============================================================
# 🧾 CREAR TABLA RAW
# ============================================================

def create_raw_table(
    engine: Engine,
    sample_df: pd.DataFrame,
    drop_if_exists: bool = False
):
    """
    Crea la tabla RAW dinámicamente a partir
    de las columnas retornadas por la API.

    Parameters
    ----------
    engine : Engine

    sample_df : pd.DataFrame

    drop_if_exists : bool
        Si es True elimina la tabla antes de crearla.
    """

    with engine.begin() as conn:

        # --------------------------------------------------
        # DROP TABLE
        # --------------------------------------------------
        if drop_if_exists:

            conn.execute(
                text(f"DROP TABLE IF EXISTS {RAW_TABLE}")
            )

            print("⚠️ Tabla RAW eliminada")

        # --------------------------------------------------
        # COLUMNAS DINÁMICAS
        # --------------------------------------------------
        columns_sql = []

        for col in sample_df.columns:

            safe_col = col.replace("-", "_")

            if col == "encounter_id":
                col_type = "BIGINT"

            elif pd.api.types.is_numeric_dtype(sample_df[col]):
                col_type = "DOUBLE"

            else:
                col_type = "TEXT"

            columns_sql.append(
                f"`{safe_col}` {col_type}"
            )

        # --------------------------------------------------
        # METADATA
        # --------------------------------------------------
        metadata_cols = [
            "`load_id` VARCHAR(100)",
            "`load_timestamp` DATETIME",
            "`source` VARCHAR(100)",
            "`record_status` VARCHAR(50)"
        ]

        columns_sql.extend(metadata_cols)

        # --------------------------------------------------
        # CREATE TABLE
        # --------------------------------------------------
        create_sql = f"""
        CREATE TABLE {RAW_TABLE} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            {",".join(columns_sql)},
            UNIQUE (`encounter_id`)
        )
        """

        conn.execute(text(create_sql))

    print("✅ Tabla RAW creada correctamente")


# ============================================================
# 🧾 CREAR TABLA RAW
# ============================================================

def create_raw_table(
    engine: Engine,
    sample_df: pd.DataFrame,
    drop_if_exists: bool = False
):
    """
    Crea la tabla RAW dinámicamente.

    Parameters
    ----------
    engine : Engine

    sample_df : pd.DataFrame

    drop_if_exists : bool
        Si es True elimina la tabla antes de crearla.
    """

    with engine.begin() as conn:

        # --------------------------------------------------
        # DROP TABLE
        # --------------------------------------------------
        if drop_if_exists:

            conn.execute(
                text(f"DROP TABLE IF EXISTS {RAW_TABLE}")
            )

            print("⚠️ Tabla RAW eliminada")

        # --------------------------------------------------
        # COLUMNAS
        # --------------------------------------------------
        columns_sql = []

        for col in sample_df.columns:

            safe_col = (
                col.replace("-", "_")
                .replace(" ", "_")
                .lower()
            )

            if col == "encounter_id":
                col_type = "BIGINT"

            elif pd.api.types.is_numeric_dtype(
                sample_df[col]
            ):
                col_type = "DOUBLE"

            else:
                col_type = "TEXT"

            columns_sql.append(
                f"`{safe_col}` {col_type}"
            )

        # --------------------------------------------------
        # METADATA
        # --------------------------------------------------
        metadata_cols = [
            "`load_id` VARCHAR(100)",
            "`load_timestamp` DATETIME",
            "`source` VARCHAR(100)",
            "`record_status` VARCHAR(50)"
        ]

        columns_sql.extend(metadata_cols)

        # --------------------------------------------------
        # CREATE SQL
        # --------------------------------------------------
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            {",".join(columns_sql)},
            UNIQUE (`encounter_id`)
        )
        """

        conn.execute(text(create_sql))

    print("✅ Tabla RAW validada")


# ============================================================
# 🔍 VALIDACIONES BÁSICAS
# ============================================================

def validate_raw_data(df: pd.DataFrame):
    """
    Ejecuta validaciones básicas de calidad.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a validar.
    """

    if df.empty:
        raise Exception("DataFrame vacío")

    if "encounter_id" not in df.columns:
        raise Exception("No existe encounter_id")

    duplicated = df["encounter_id"].duplicated().sum()

    if duplicated > 0:
        raise Exception(
            f"Existen {duplicated} duplicados"
        )

    print("✅ Validaciones básicas completadas")


# ============================================================
# 🧼 PREPROCESAMIENTO CLEAN
# ============================================================

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta transformaciones básicas sobre los datos.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame RAW.

    Returns
    -------
    pd.DataFrame
        DataFrame transformado.
    """

    df = df.copy()

    # ----------------------------------------
    # NORMALIZAR MISSING
    # ----------------------------------------
    df = normalize_missing_values(df)

    # ----------------------------------------
    # CONVERTIR COLUMNAS NUMÉRICAS
    # ----------------------------------------
    numeric_cols = [
        "time_in_hospital",
        "num_lab_procedures",
        "num_procedures",
        "num_medications",
        "number_outpatient",
        "number_emergency",
        "number_inpatient",
        "number_diagnoses"
    ]

    for col in numeric_cols:

        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    # ----------------------------------------
    # TARGET BINARIO
    # ----------------------------------------
    if "readmitted" in df.columns:

        df["target"] = (
            df["readmitted"]
            .apply(lambda x: 1 if x in ["<30", ">30"] else 0)
        )

    return df


# ============================================================
# 🎯 GENERAR SPLITS TRAIN/VAL/TEST
# ============================================================

def assign_dataset_split(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Asigna cada registro a train/validation/test.

    Si alguna clase tiene muy pocos ejemplos,
    se realiza un split sin estratificación.

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    df = df.copy()

    # --------------------------------------------------------
    # VALIDAR TARGET
    # --------------------------------------------------------
    if "target" not in df.columns:
        raise Exception(
            "La columna target no existe"
        )

    # --------------------------------------------------------
    # CONTEO CLASES
    # --------------------------------------------------------
    class_counts = df["target"].value_counts()

    print("\n📊 Distribución target:")
    print(class_counts)

    # --------------------------------------------------------
    # VALIDAR SI SE PUEDE ESTRATIFICAR
    # --------------------------------------------------------
    can_stratify = class_counts.min() >= 2

    if can_stratify:

        print("✅ Split estratificado")

        train_df, temp_df = train_test_split(
            df,
            test_size=(1 - TRAIN_SIZE),
            random_state=42,
            stratify=df["target"]
        )

        val_df, test_df = train_test_split(
            temp_df,
            test_size=0.5,
            random_state=42,
            stratify=temp_df["target"]
        )

    else:

        print(
            "⚠️ Muy pocos ejemplos por clase. "
            "Split sin estratificación."
        )

        train_df, temp_df = train_test_split(
            df,
            test_size=(1 - TRAIN_SIZE),
            random_state=42
        )

        val_df, test_df = train_test_split(
            temp_df,
            test_size=0.5,
            random_state=42
        )

    # --------------------------------------------------------
    # ASIGNAR LABELS
    # --------------------------------------------------------
    train_df["dataset_split"] = "train"

    val_df["dataset_split"] = "validation"

    test_df["dataset_split"] = "test"

    # --------------------------------------------------------
    # CONCATENAR
    # --------------------------------------------------------
    final_df = pd.concat([
        train_df,
        val_df,
        test_df
    ])

    print("\n✅ Split completado")
    print(
        final_df["dataset_split"]
        .value_counts()
    )

    return final_df


def create_clean_table(
    engine,
    sample_df,
    drop_if_exists=True
):
    """
    Crea la tabla CLEAN_DATA.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        Conexión a MySQL.

    sample_df : pd.DataFrame
        DataFrame ejemplo para inferir columnas.

    drop_if_exists : bool
        Si True elimina la tabla antes de crearla.
    """

    # ======================================================
    # COPIA SEGURA
    # ======================================================

    df = sample_df.copy()

    # ======================================================
    # ELIMINAR COLUMNAS TÉCNICAS DUPLICADAS
    # ======================================================

    forbidden_columns = {
        "id"
    }

    valid_columns = [
        col for col in df.columns
        if col.lower() not in forbidden_columns
    ]

    df = df[valid_columns]

    # ======================================================
    # CREAR TABLA
    # ======================================================

    with engine.begin() as conn:

        # --------------------------------------------------
        # DROP TABLE
        # --------------------------------------------------

        if drop_if_exists:
            conn.execute(
                text(
                    f"DROP TABLE IF EXISTS {CLEAN_TABLE}"
                )
            )

        # --------------------------------------------------
        # VALIDAR SI EXISTE
        # --------------------------------------------------

        table_exists = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = '{CLEAN_TABLE}'
            """)
        ).scalar()

        if table_exists:
            print("ℹ️ Tabla CLEAN ya existe")
            return

        # --------------------------------------------------
        # SQL COLUMNAS
        # --------------------------------------------------

        sql_columns = []

        for col in df.columns:

            dtype = str(df[col].dtype).lower()

            if "int" in dtype:
                sql_type = "BIGINT"

            elif "float" in dtype:
                sql_type = "DOUBLE"

            elif "datetime" in dtype:
                sql_type = "DATETIME"

            else:
                sql_type = "TEXT"

            sql_columns.append(
                f"`{col}` {sql_type}"
            )

        columns_sql = ",\n".join(sql_columns)

        # --------------------------------------------------
        # CREATE SQL
        # --------------------------------------------------

        create_sql = f"""
        CREATE TABLE {CLEAN_TABLE} (

            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            {columns_sql},

            UNIQUE (`encounter_id`)
        )
        """

        conn.execute(
            text(create_sql)
        )

    print("✅ Tabla CLEAN creada")


# ============================================================
# 💾 INSERTAR CLEAN DATA
# ============================================================

def insert_clean_data(
    engine,
    clean_df
):
    """
    Inserta datos en CLEAN_DATA.
    """

    df = clean_df.copy()

    # ======================================================
    # ELIMINAR ID TÉCNICO
    # ======================================================

    df = df.drop(
        columns=["id"],
        errors="ignore"
    )

    # ======================================================
    # INSERT
    # ======================================================

    df.to_sql(
        CLEAN_TABLE,
        con=engine,
        if_exists="append",
        index=False
    )

    print(
        f"✅ {len(df)} registros insertados en CLEAN"
    )


# ============================================================
# 📦 CARGA INCREMENTAL RAW
# ============================================================

def insert_raw_incremental(
    engine: Engine,
    df: pd.DataFrame,
    source: str = "api"
):
    """
    Inserta registros nuevos en la tabla RAW.

    Los duplicados se controlan mediante encounter_id.

    Parameters
    ----------
    engine : Engine
        Engine SQLAlchemy.

    df : pd.DataFrame
        Datos a insertar.

    source : str
        Origen de los datos.
    """

    if df.empty:
        print("⚠️ DataFrame vacío")
        return

    load_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    df["load_id"] = load_id
    df["load_timestamp"] = datetime.now()
    df["source"] = source
    df["record_status"] = "RAW"

    existing_ids_query = f"""
    SELECT encounter_id
    FROM {RAW_TABLE}
    """

    existing_df = pd.read_sql(
        existing_ids_query,
        engine
    )

    existing_ids = set(existing_df["encounter_id"].tolist())

    df = df[
        ~df["encounter_id"].isin(existing_ids)
    ]

    if df.empty:
        print("⚠️ No hay nuevos registros")
        return

    df.to_sql(
        RAW_TABLE,
        engine,
        if_exists="append",
        index=False
    )

    print(f"✅ Insertados {len(df)} registros RAW")


def task_validate_source():
    validate_api_health()
    return True

def task_ingest_raw():
    engine = create_engine_connection()

    df = fetch_batch(BATCH_SIZE)
    df = normalize_column_names(df)

    validate_raw_data(df)

    create_raw_table(engine, df, drop_if_exists=False)
    insert_raw_incremental(engine, df)

    return len(df)

def task_validate_raw():
    engine = create_engine_connection()

    df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", engine)

    validate_raw_data(df)

    return {
        "rows": len(df),
        "duplicates": int(df["encounter_id"].duplicated().sum())
    }

def task_preprocess():
    engine = create_engine_connection()

    raw_df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", engine)

    clean_df = preprocess_data(raw_df)

    return len(clean_df)

def task_store_clean():
    engine = create_engine_connection()

    raw_df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", engine)

    clean_df = preprocess_data(raw_df)

    create_clean_table(engine, clean_df, drop_if_exists=True)
    insert_clean_data(engine, clean_df)

    return len(clean_df)

def task_split_data():
    engine = create_engine_connection()

    df = pd.read_sql(f"SELECT * FROM {CLEAN_TABLE}", engine)

    df = assign_dataset_split(df)

    # mejor práctica: guardar en misma tabla o tabla separada
    df.to_sql(
        "dataset_split",
        engine,
        if_exists="replace",
        index=False
    )

    return df["dataset_split"].value_counts().to_dict()


def create_db_connection():
    """
    Creates a SQLAlchemy connection to MySQL.
    """

    connection_uri = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )

    return create_engine(connection_uri)


def ensure_minio_bucket():
    """
    Creates the MinIO bucket if it does not exist.
    """

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    print(f"Bucket '{MINIO_BUCKET}' ready.")


def load_clean_data():
    """
    Loads clean processed data from MySQL.
    """

    engine = create_db_connection()

    query = f"""
    SELECT *
    FROM {SPLIT_TABLE}
    """

    df = pd.read_sql(query, engine)

    return df


def validate_dataset(df):
    """
    Validates dataset integrity before training.
    """

    if df.empty:
        raise ValueError("Dataset is empty.")

    if TARGET_COLUMN not in df.columns:
        raise ValueError("Target column not found.")

    if "dataset_split" not in df.columns:
        raise ValueError("dataset_split column missing.")



def split_dataset(df):
    """
    Splits dataset using dataset_split column.
    """

    train_df = df[df["dataset_split"] == "train"]
    val_df = df[df["dataset_split"] == "validation"]
    test_df = df[df["dataset_split"] == "test"]

    return train_df, val_df, test_df



# ============================================================
# FEATURES A EXCLUIR DEL ENTRENAMIENTO
# ============================================================

DROP_COLUMNS = [

    # ----------------------------------------
    # TARGETS
    # ----------------------------------------
    "readmitted",
    TARGET_COLUMN,

    # ----------------------------------------
    # SPLIT
    # ----------------------------------------
    "dataset_split",

    # ----------------------------------------
    # IDs TÉCNICOS
    # ----------------------------------------
    "id",
    "encounter_id",
    "patient_nbr",

    # ----------------------------------------
    # METADATA PIPELINE
    # ----------------------------------------
    "load_id",
    "load_timestamp",
    "source",
    "record_status"
]
def prepare_features(df):
    """
    Prepares dataset for CatBoost training.
    """

    # ======================================================
    # FEATURES / TARGET
    # ======================================================

    X = df.drop(
        columns=DROP_COLUMNS,
        errors="ignore"
    ).copy()

    y = df[TARGET_COLUMN].copy()

    # ======================================================
    # DETECTAR CATEGÓRICAS
    # ======================================================

    categorical_columns = (
        X.select_dtypes(
            include=["object", "category"]
        )
        .columns
        .tolist()
    )


    FORCE_CATEGORICAL_COLUMNS = [
    "diag_1",
    "diag_2",
    "diag_3",
    "admission_type_id",
    "discharge_disposition_id",
    "admission_source_id"
    
    ]

    
    # ======================================================
    # LIMPIAR CATEGÓRICAS
    # ======================================================

    for col in categorical_columns:

        X[col] = (
            X[col]
            .astype(str)
            .replace(
                {
                    "None": "missing",
                    "nan": "missing",
                    "NaN": "missing",
                    "<NA>": "missing",
                    "?": "missing"
                }
            )
        )

    for col in FORCE_CATEGORICAL_COLUMNS:

        if col in X.columns:
    
            X[col] = (
                X[col]
                .astype(str)
                .replace(
                    {
                        "nan": "missing",
                        "None": "missing"
                    }
                )
            )

    # ======================================================
    # LIMPIAR NUMÉRICAS
    # ======================================================

    numeric_columns = [
        col for col in X.columns
        if col not in categorical_columns
    ]

    for col in numeric_columns:

        X[col] = pd.to_numeric(
            X[col],
            errors="coerce"
        )

    # ======================================================
    # ELIMINAR COLUMNAS TOTALMENTE VACÍAS
    # ======================================================

    empty_columns = [
        col for col in X.columns
        if X[col].isna().all()
    ]

    if empty_columns:

        print(
            f"⚠️ Eliminando columnas vacías: "
            f"{empty_columns}"
        )

        X = X.drop(columns=empty_columns)

    # ======================================================
    # INFO DEBUG
    # ======================================================

    print("\n📊 Dataset preparado")
    print(f"Features: {X.shape[1]}")
    print(f"Rows: {X.shape[0]}")

    return X, y


def extract_feature_metadata(X):
    """
    Extracts feature metadata for inference validation,
    frontend generation and schema inspection.
    """

    metadata = {}

    for col in X.columns:

        # ==================================================
        # CATEGORICAL
        # ==================================================

        if (
            X[col].dtype == "object"
            or str(X[col].dtype) == "category"
        ):

            values = (
                X[col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            metadata[col] = {
                "type": "categorical",
                "values": sorted(values)
            }

        # ==================================================
        # NUMERIC
        # ==================================================

        else:

            numeric_series = pd.to_numeric(
                X[col],
                errors="coerce"
            )

            metadata[col] = {
                "type": "numeric",
                "min": float(numeric_series.min())
                if not numeric_series.isna().all()
                else None,

                "max": float(numeric_series.max())
                if not numeric_series.isna().all()
                else None
            }

    return metadata


def train_catboost_model(
    X_train,
    y_train,
    X_val,
    y_val
):
    """
    Trains a CatBoost classifier.
    """

    categorical_features = (
        X_train.select_dtypes(include=["object", "category"])
        .columns
        .tolist()
    )

    model = CatBoostClassifier(
        iterations=300,
        learning_rate=0.1,
        depth=10,
        eval_metric="F1",
        loss_function="Logloss",
        verbose=100
    )

    model.fit(
        X_train,
        y_train,
        cat_features=categorical_features,
        eval_set=(X_val, y_val)
    )

    return model


from tempfile import NamedTemporaryFile
from minio import Minio
import json
import os

def save_feature_metadata_to_minio(
    feature_metadata,
    object_prefix="training"
):
    """
    Saves feature metadata to MinIO.
    """

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # ==================================================
    # CREATE TEMP FILE
    # ==================================================

    tmp_file = NamedTemporaryFile(
        suffix=".json",
        mode="w",
        delete=False
    )

    try:

        json.dump(
            feature_metadata,
            tmp_file,
            indent=2
        )

        tmp_file.flush()
        tmp_file.close()

        client.fput_object(
            MINIO_BUCKET,
            f"{object_prefix}/feature_metadata.json",
            tmp_file.name
        )

    finally:

        if os.path.exists(tmp_file.name):
            os.remove(tmp_file.name)

    print("✅ Feature metadata saved")



def evaluate_model(model, X_test, y_test):
    """
    Evaluates the trained model.
    """

    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": accuracy_score(y_test, predictions),
        "precision": precision_score(y_test, predictions),
        "recall": recall_score(y_test, predictions),
        "f1_score": f1_score(y_test, predictions),
        "roc_auc": roc_auc_score(y_test, probabilities)
    }

    return metrics


def load_feature_metadata_from_minio(
    object_prefix="training"
):
    """
    Loads feature metadata from MinIO.
    """

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    metadata_path = "/tmp/feature_metadata.json"

    client.fget_object(
        MINIO_BUCKET,
        f"{object_prefix}/feature_metadata.json",
        metadata_path
    )

    with open(metadata_path, "r") as f:

        metadata = json.load(f)

    return metadata


def setup_mlflow():
    """
    Configures MLflow tracking.
    """

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)


def register_model(
    model,
    metrics,
    params,
    X_example,
    feature_metadata
):
    """
    Logs model and metadata into MLflow.
    """

    with mlflow.start_run():

        mlflow.log_params(params)

        mlflow.log_metrics(metrics)

        # ==================================================
        # SIGNATURE
        # ==================================================

        predictions = model.predict(X_example)

        signature = infer_signature(
            X_example,
            predictions
        )

        # ==================================================
        # SAVE FEATURE METADATA
        # ==================================================

        with tempfile.TemporaryDirectory() as tmp_dir:
        
            metadata_path = os.path.join(
                tmp_dir,
                "feature_metadata.json"
            )
        
            with open(metadata_path, "w") as f:
        
                json.dump(
                    feature_metadata,
                    f,
                    indent=2
                )
        
            mlflow.log_artifact(
                metadata_path,
                artifact_path="metadata"
            )

        # ==================================================
        # LOG MODEL
        # ==================================================

        mlflow.catboost.log_model(
            cb_model=model,
            name="model",
            registered_model_name="diabetes_catboost_model",
            signature=signature,
            input_example=X_example.head(3)
        )


def compare_and_promote_model(
    current_f1
):
    """
    Promotes the best model to champion alias.
    """

    client = MlflowClient()

    model_name = "diabetes_catboost_model"

    latest_versions = client.search_model_versions(
        f"name='{model_name}'"
    )

    best_version = None
    best_score = -1

    for version in latest_versions:

        run_id = version.run_id

        run = client.get_run(run_id)

        score = run.data.metrics.get("f1_score", 0)

        if score > best_score:
            best_score = score
            best_version = version.version

    if best_version:

        client.set_registered_model_alias(
            model_name,
            "champion",
            best_version
        )

        print(
            f"Model version {best_version} promoted "
            f"with F1-score={best_score:.4f}"
        )



def debug_catboost_columns(X):
    """
    Detect problematic columns for CatBoost.
    """

    print("\n===== DEBUGGING COLUMNS =====\n")

    for col in X.columns:

        dtype = X[col].dtype

        print(f"\nCOLUMN: {col}")
        print(f"DTYPE: {dtype}")

        try:
            sample_values = X[col].dropna().head(10).tolist()

            print("SAMPLE VALUES:")
            print(sample_values)

            print("PYTHON TYPES:")
            print(
                X[col]
                .dropna()
                .head(10)
                .apply(type)
                .tolist()
            )

        except Exception as e:
            print(f"ERROR INSPECTING COLUMN: {e}")



from tempfile import NamedTemporaryFile
from minio import Minio
import json
import os

def save_model_to_minio(
    model,
    metrics,
    raw_example_df,
    object_prefix="training"
):
    """
    Saves model, metrics and schema example to MinIO.
    """

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # ==================================================
    # SAVE MODEL
    # ==================================================

    tmp_model = NamedTemporaryFile(
        suffix=".cbm",
        delete=False
    )

    tmp_model.close()

    try:

        model.save_model(tmp_model.name)

        client.fput_object(
            MINIO_BUCKET,
            f"{object_prefix}/model.cbm",
            tmp_model.name
        )

    finally:

        if os.path.exists(tmp_model.name):
            os.remove(tmp_model.name)

    # ==================================================
    # SAVE METRICS
    # ==================================================

    tmp_metrics = NamedTemporaryFile(
        suffix=".json",
        mode="w",
        delete=False
    )

    try:

        json.dump(metrics, tmp_metrics)

        tmp_metrics.flush()
        tmp_metrics.close()

        client.fput_object(
            MINIO_BUCKET,
            f"{object_prefix}/metrics.json",
            tmp_metrics.name
        )

    finally:

        if os.path.exists(tmp_metrics.name):
            os.remove(tmp_metrics.name)

    # ==================================================
    # SAVE RAW INPUT EXAMPLE
    # ==================================================

    tmp_input = NamedTemporaryFile(
        suffix=".parquet",
        delete=False
    )

    tmp_input.close()

    try:

        raw_example_df.to_parquet(
            tmp_input.name,
            index=False
        )

        client.fput_object(
            MINIO_BUCKET,
            f"{object_prefix}/input_example.parquet",
            tmp_input.name
        )

    finally:

        if os.path.exists(tmp_input.name):
            os.remove(tmp_input.name)

    print("✅ Model, metrics and schema saved")


def load_model_from_minio(
    object_prefix="training"
):
    """
    Loads model, metrics and schema example from MinIO.
    """

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # ==================================================
    # MODEL
    # ==================================================

    model_path = "/tmp/model.cbm"

    client.fget_object(
        MINIO_BUCKET,
        f"{object_prefix}/model.cbm",
        model_path
    )

    model = CatBoostClassifier()

    model.load_model(model_path)

    # ==================================================
    # METRICS
    # ==================================================

    metrics_path = "/tmp/metrics.json"

    client.fget_object(
        MINIO_BUCKET,
        f"{object_prefix}/metrics.json",
        metrics_path
    )

    with open(metrics_path, "r") as f:
        metrics = json.load(f)

    # ==================================================
    # RAW INPUT EXAMPLE
    # ==================================================

    example_path = "/tmp/input_example.parquet"

    client.fget_object(
        MINIO_BUCKET,
        f"{object_prefix}/input_example.parquet",
        example_path
    )

    raw_example_df = pd.read_parquet(
        example_path
    )

    # IMPORTANT:
    # Reapply preprocessing
    X_example, _ = prepare_features(
        raw_example_df
    )

    return model, metrics, X_example


def task_train_model():
    """
    DAG Task:
    Train model and persist artifacts.
    """

    print("\n===== TASK 7: TRAIN MODEL =====")

    df = load_clean_data()

    validate_dataset(df)

    train_df, val_df, test_df = split_dataset(df)

    X_train, y_train = prepare_features(train_df)
    X_val, y_val = prepare_features(val_df)
    X_test, y_test = prepare_features(test_df)

    # ==================================================
    # FEATURE METADATA
    # ==================================================

    feature_metadata = extract_feature_metadata(
        X_train
    )

    # ==================================================
    # TRAIN MODEL
    # ==================================================

    model = train_catboost_model(
        X_train,
        y_train,
        X_val,
        y_val
    )

    metrics = evaluate_model(
        model,
        X_test,
        y_test
    )

    # ==================================================
    # SAVE MODEL
    # ==================================================

    save_model_to_minio(
        model,
        metrics,
        train_df.head(50)
    )

    # ==================================================
    # SAVE FEATURE METADATA
    # ==================================================

    save_feature_metadata_to_minio(
        feature_metadata
    )

    print(metrics)


def task_register_model():
    """
    DAG Task:
    Register trained model into MLflow.
    """

    print("\n===== TASK 8: REGISTER MODEL =====")

    setup_mlflow()

    model, metrics, X_example = load_model_from_minio()

    feature_metadata = (
        load_feature_metadata_from_minio()
    )

    params = model.get_params()

    register_model(
        model,
        metrics,
        params,
        X_example,
        feature_metadata
    )

    print("✅ Model registered in MLflow")



def task_compare_and_promote():
    """
    DAG Task:
    Compare models and promote best version.
    """

    print("\n===== TASK 9-10: COMPARE AND PROMOTE =====")

    client = MlflowClient()

    model_name = "diabetes_catboost_model"

    versions = client.search_model_versions(
        f"name='{model_name}'"
    )

    best_version = None
    best_score = -1

    for version in versions:

        run = client.get_run(version.run_id)

        score = run.data.metrics.get(
            "f1_score",
            0
        )

        print(
            f"Version={version.version} "
            f"F1={score}"
        )

        if score > best_score:

            best_score = score
            best_version = version.version

    if best_version:

        client.set_registered_model_alias(
            model_name,
            "champion",
            best_version
        )

        print(
            f"\n🏆 Champion updated "
            f"-> version {best_version}"
        )


def run_pipeline():
    """
    Executes the complete training pipeline.
    """

    try:

        print("Starting training pipeline...")

        ensure_minio_bucket()

        setup_mlflow()

        df = load_clean_data()

        validate_dataset(df)

        train_df, val_df, test_df = split_dataset(df)

        X_train, y_train = prepare_features(train_df)
        X_val, y_val = prepare_features(val_df)
        X_test, y_test = prepare_features(test_df)
        
        #debug_catboost_columns(X_train)
        
        model = train_catboost_model(
                X_train,
                y_train,
                X_val,
                y_val
        )

        metrics = evaluate_model(
            model,
            X_test,
            y_test
        )

        params = model.get_params()

        register_model(
            model,
            metrics,
            params
        )

        compare_and_promote_model(
            metrics["f1_score"]
        )

        print("Pipeline completed successfully.")

        return metrics

    except Exception as error:

        print(f"Pipeline failed: {error}")

        raise


