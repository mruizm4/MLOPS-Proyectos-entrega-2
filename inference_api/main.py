# ============================================================
# 📦 IMPORTS
# ============================================================

import os
import json
import time
import tempfile

from datetime import datetime

import pandas as pd
import mlflow
import mlflow.catboost

from mlflow.tracking import MlflowClient

from sqlalchemy import (
    create_engine,
    text
)

from fastapi import (
    FastAPI,
    HTTPException
)

from fastapi.responses import JSONResponse

# ============================================================
# ⚙️ CONFIG
# ============================================================

# ----------------------------------------
# MYSQL
# ----------------------------------------

MYSQL_HOST = "mysql_db"
MYSQL_PORT = 3306
MYSQL_DB = "mlops_db"
MYSQL_USER = "mlops_user"
MYSQL_PASSWORD = "mlops_pass"

# ----------------------------------------
# MLFLOW
# ----------------------------------------

MLFLOW_TRACKING_URI = "http://mlflow:5000"

MODEL_NAME = "diabetes_catboost_model"

MODEL_ALIAS = "champion"

# ----------------------------------------
# MODEL REFRESH
# ----------------------------------------

MODEL_REFRESH_INTERVAL = 1

# ============================================================
# 🚀 FASTAPI
# ============================================================

app = FastAPI(
    title="Diabetes Inference API",
    version="1.0.0"
)

# ============================================================
# 🌎 GLOBALS
# ============================================================

MODEL = None

FEATURE_METADATA = None

MODEL_VERSION = None

LAST_MODEL_CHECK = 0

MODEL_LOADED_AT = None

# ============================================================
# 🗄️ DATABASE
# ============================================================

def create_db_engine():
    """
    Creates SQLAlchemy engine.
    """

    connection_uri = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )

    return create_engine(connection_uri)

ENGINE = create_db_engine()

# ============================================================
# 🗄️ CREATE INFERENCE TABLE
# ============================================================

def create_inference_table():
    """
    Creates inference log table.
    """

    query = """
    CREATE TABLE IF NOT EXISTS inference_logs (

        id BIGINT AUTO_INCREMENT PRIMARY KEY,

        timestamp DATETIME NOT NULL,

        model_name VARCHAR(255),

        model_version VARCHAR(50),

        model_alias VARCHAR(50),

        processing_time_ms FLOAT,

        prediction VARCHAR(255),

        probability FLOAT,

        request_json JSON,

        response_json JSON
    )
    """

    with ENGINE.begin() as conn:

        conn.execute(text(query))

    print("✅ inference_logs table ready")

# ============================================================
# 🤖 LOAD CHAMPION MODEL
# ============================================================

def load_champion_model():
    """
    Loads champion model and metadata from MLflow.
    """

    global MODEL
    global FEATURE_METADATA
    global MODEL_VERSION
    global MODEL_LOADED_AT

    print("⬇️ Loading champion model...")

    mlflow.set_tracking_uri(
        MLFLOW_TRACKING_URI
    )

    client = MlflowClient()

    champion = client.get_model_version_by_alias(
        MODEL_NAME,
        MODEL_ALIAS
    )

    version = champion.version

    run_id = champion.run_id

    # --------------------------------------------------------
    # LOAD MODEL
    # --------------------------------------------------------

    model_uri = (
        f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
    )

    MODEL = mlflow.catboost.load_model(
        model_uri
    )

    # --------------------------------------------------------
    # LOAD FEATURE METADATA
    # --------------------------------------------------------

    metadata_path = (
        mlflow.artifacts.download_artifacts(
            run_id=run_id,
            artifact_path=(
                "metadata/"
                "feature_metadata.json"
            )
        )
    )

    with open(metadata_path, "r") as f:

        FEATURE_METADATA = json.load(f)

    MODEL_VERSION = version

    MODEL_LOADED_AT = datetime.utcnow()

    print(
        f"✅ Champion model loaded "
        f"(version={version})"
    )

# ============================================================
# 🔄 REFRESH MODEL IF NEEDED
# ============================================================
def refresh_model_if_needed():
    """
    Reloads model if champion changed.
    Also tries loading model if none exists.
    """

    global LAST_MODEL_CHECK
    global MODEL

    now = time.time()

    # --------------------------------------------------------
    # IF MODEL IS MISSING:
    # ALWAYS TRY
    # --------------------------------------------------------

    if MODEL is not None:

        if (
            now - LAST_MODEL_CHECK
            < MODEL_REFRESH_INTERVAL
        ):

            return

    LAST_MODEL_CHECK = now

    try:

        mlflow.set_tracking_uri(
            MLFLOW_TRACKING_URI
        )

        client = MlflowClient()

        champion = (
            client.get_model_version_by_alias(
                MODEL_NAME,
                MODEL_ALIAS
            )
        )

        latest_version = champion.version

        # ----------------------------------------------------
        # NO MODEL LOADED
        # ----------------------------------------------------

        if MODEL is None:

            print(
                "⬇️ Loading first champion model..."
            )

            load_champion_model()

            return

        # ----------------------------------------------------
        # RELOAD IF VERSION CHANGED
        # ----------------------------------------------------

        global MODEL_VERSION

        if (
            str(latest_version)
            != str(MODEL_VERSION)
        ):

            print(
                f"🔄 New champion detected "
                f"({MODEL_VERSION} -> "
                f"{latest_version})"
            )

            load_champion_model()

    except Exception as e:

        print(
            "⚠️ Could not refresh model"
        )

        print(str(e))

# ============================================================
# ✅ VALIDATE PAYLOAD
# ============================================================

def validate_payload(payload):
    """
    Validates payload using feature metadata.
    """

    validated = {}

    # --------------------------------------------------------
    # CHECK REQUIRED FEATURES
    # --------------------------------------------------------

    for feature_name, metadata in (
        FEATURE_METADATA.items()
    ):

        if feature_name not in payload:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"Missing feature: "
                    f"{feature_name}"
                )
            )

        value = payload[feature_name]

        # ----------------------------------------------------
        # CATEGORICAL
        # ----------------------------------------------------

        if metadata["type"] == "categorical":

            allowed_values = metadata["values"]

            if value not in allowed_values:

                raise HTTPException(
                    status_code=400,
                    detail={
                        "feature": feature_name,
                        "invalid_value": value,
                        "allowed_values": allowed_values
                    }
                )

            validated[feature_name] = str(value)

        # ----------------------------------------------------
        # NUMERIC
        # ----------------------------------------------------

        else:

            try:

                numeric_value = float(value)

            except Exception:

                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid numeric value "
                        f"for {feature_name}"
                    )
                )

            min_value = metadata["min"]
            max_value = metadata["max"]

            if (
                min_value is not None
                and numeric_value < min_value
            ):

                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"{feature_name} below "
                        f"minimum value"
                    )
                )

            if (
                max_value is not None
                and numeric_value > max_value
            ):

                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"{feature_name} above "
                        f"maximum value"
                    )
                )

            validated[feature_name] = numeric_value

    return validated

# ============================================================
# 🗄️ LOG INFERENCE
# ============================================================

def log_inference(
    request_json,
    response_json,
    prediction,
    probability,
    processing_time_ms
):
    """
    Stores inference into MySQL.
    """

    query = text("""
    INSERT INTO inference_logs (

        timestamp,
        model_name,
        model_version,
        model_alias,
        processing_time_ms,
        prediction,
        probability,
        request_json,
        response_json

    ) VALUES (

        :timestamp,
        :model_name,
        :model_version,
        :model_alias,
        :processing_time_ms,
        :prediction,
        :probability,
        :request_json,
        :response_json
    )
    """)

    with ENGINE.begin() as conn:

        conn.execute(
            query,
            {
                "timestamp": datetime.utcnow(),
                "model_name": MODEL_NAME,
                "model_version": str(MODEL_VERSION),
                "model_alias": MODEL_ALIAS,
                "processing_time_ms": processing_time_ms,
                "prediction": str(prediction),
                "probability": float(probability),
                "request_json": json.dumps(request_json),
                "response_json": json.dumps(response_json)
            }
        )

# ============================================================
# 🚀 STARTUP
# ============================================================

@app.on_event("startup")
def startup_event():

    print("🚀 Starting inference API...")

    create_inference_table()

    try:

        load_champion_model()

    except Exception as e:

        print(
            "⚠️ No champion model available yet"
        )

        print(str(e))

    print("✅ API ready")

# ============================================================
# ❤️ HEALTH
# ============================================================


@app.get("/health")
def health():

    # --------------------------------------------------------
    # TRY REFRESH MODEL
    # --------------------------------------------------------

    refresh_model_if_needed()

    return {
        "status": (
            "ok"
            if MODEL is not None
            else "degraded"
        ),
        "model_loaded": MODEL is not None,
        "model_version": MODEL_VERSION,
        "model_loaded_at": MODEL_LOADED_AT
    }


# ============================================================
# 🤖 MODEL INFO
# ============================================================

@app.get("/model-info")
def model_info():

    return {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "model_alias": MODEL_ALIAS,
        "loaded_at": MODEL_LOADED_AT
    }

# ============================================================
# 📊 PROMETHEUS METRICS
# ============================================================

@app.get("/metrics")
def metrics():
    """
    Dummy Prometheus endpoint.
    """

    return JSONResponse(
        content={
            "message": (
                "Prometheus metrics "
                "not enabled yet"
            )
        }
    )

# ============================================================
# 🔮 PREDICT
# ============================================================

@app.post("/predict")
def predict(payload: dict):

    # --------------------------------------------------------
    # REFRESH MODEL
    # --------------------------------------------------------

    refresh_model_if_needed()

    # --------------------------------------------------------
    # TIMER
    # --------------------------------------------------------

    start_time = time.time()

    if MODEL is None:

        raise HTTPException(
            status_code=503,
            detail=(
                "No champion model available"
            )
        )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    validated_payload = validate_payload(
        payload
    )

    # --------------------------------------------------------
    # BUILD DATAFRAME
    # --------------------------------------------------------

    input_df = pd.DataFrame(
        [validated_payload]
    )

    

    # --------------------------------------------------------
    # PREDICT
    # --------------------------------------------------------

    prediction = MODEL.predict(
        input_df
    )[0]

    probability = (
        MODEL.predict_proba(input_df)[0][1]
    )

    # --------------------------------------------------------
    # PROCESSING TIME
    # --------------------------------------------------------

    processing_time_ms = (
        time.time() - start_time
    ) * 1000

    # --------------------------------------------------------
    # RESPONSE
    # --------------------------------------------------------

    response = {
        "prediction": int(prediction),
        "probability": float(probability),
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "model_alias": MODEL_ALIAS,
        "processing_time_ms": round(
            processing_time_ms,
            2
        )
    }

    # --------------------------------------------------------
    # LOG INFERENCE
    # --------------------------------------------------------

    log_inference(
        request_json=payload,
        response_json=response,
        prediction=prediction,
        probability=probability,
        processing_time_ms=processing_time_ms
    )

    return response