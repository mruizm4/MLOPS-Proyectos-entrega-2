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

from prometheus_fastapi_instrumentator import (
    Instrumentator
)

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST
)


from mlflow.tracking import MlflowClient

from sqlalchemy import (
    create_engine,
    text
)

from fastapi import (
    FastAPI,
    HTTPException
)
import random

from fastapi.responses import Response

from fastapi.responses import JSONResponse

# ============================================================
# ⚙️ CONFIG
# ============================================================

# ----------------------------------------
# MYSQL
# ----------------------------------------

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql_db")
MYSQL_PORT = os.environ.get("MYSQL_PORT", 3306)
MYSQL_DB = os.environ.get("MYSQL_DATABASE")
MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")

# ----------------------------------------
# MLFLOW
# ----------------------------------------

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

MODEL_NAME = os.environ.get("MODEL_NAME", "diabetes_model")

MODEL_ALIAS = os.environ.get("MODEL_ALIAS", "champion")

# ----------------------------------------
# MODEL REFRESH
# ----------------------------------------

MODEL_REFRESH_INTERVAL = 1

# ============================================================
# 📊 PROMETHEUS METRICS
# ============================================================

REQUEST_COUNT = Counter(
    "inference_requests_total",
    "Total inference requests"
)

PREDICTION_COUNT = Counter(
    "predictions_total",
    "Total successful predictions"
)

PREDICTION_ERRORS = Counter(
    "prediction_errors_total",
    "Total prediction errors"
)

REQUEST_LATENCY = Histogram(
    "prediction_latency_seconds",
    "Prediction latency"
)

MODEL_LOADED = Gauge(
    "model_loaded",
    "Whether a model is loaded"
)

MODEL_VERSION_GAUGE =Gauge(
    "model_info",
    "Current loaded model",
    ["model_name", "model_version", "model_alias"]
)


# ============================================================
# 🚀 FASTAPI
# ============================================================

app = FastAPI(
    title="Diabetes Inference API",
    version="1.0.0"
)

Instrumentator().instrument(app).expose(app)

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


    MODEL_LOADED.set(1)

    try:
        MODEL_VERSION_GAUGE.labels(
            model_name=MODEL_NAME,
            model_version=str(MODEL_VERSION),
            model_alias=MODEL_ALIAS
        ).set(1)
    except:
        pass

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

        MODEL_LOADED.set(0)

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
# 📊 PROMETHEUS METRICS ENDPOINT
# ============================================================

@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )
# ============================================================
# 🔮 PREDICT
# ============================================================

@app.post("/predict")
def predict(payload: dict):

    start_time = time.time()
    REQUEST_COUNT.inc()

    try:

        refresh_model_if_needed()

        if MODEL is None:

            PREDICTION_ERRORS.inc()
            ERROR_COUNT.inc()
            raise HTTPException(
                status_code=503,
                detail="No champion model available"
            )

        validated_payload = validate_payload(
            payload
        )

        input_df = pd.DataFrame(
            [validated_payload]
        )

        prediction = MODEL.predict(
            input_df
        )[0]

        PREDICTION_COUNT.inc()

        probability = (
            MODEL.predict_proba(input_df)[0][1]
        )

        processing_time_ms = (
            time.time() - start_time
        ) * 1000



        # ----------------------------------------------------
        # PROMETHEUS
        # ----------------------------------------------------

        

        REQUEST_LATENCY.observe(
            processing_time_ms / 1000
        )
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

        log_inference(
            request_json=payload,
            response_json=response,
            prediction=prediction,
            probability=probability,
            processing_time_ms=processing_time_ms
        )

        return response

    except Exception:

        PREDICTION_ERRORS.inc()

        raise


# ============================================================
# 📦 FEATURE METADATA
# ============================================================

@app.get("/feature-metadata")
def feature_metadata():

    refresh_model_if_needed()

    if FEATURE_METADATA is None:

        raise HTTPException(
            status_code=503,
            detail=(
                "No feature metadata available"
            )
        )

    return {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "features": FEATURE_METADATA
    }


# ============================================================
# 🎲 RANDOM VALID PAYLOAD
# ============================================================

@app.get("/sample-payload")
def sample_payload():
    """
    Returns a valid payload using feature_metadata.
    Useful for testing and load testing.
    """

    refresh_model_if_needed()

    if FEATURE_METADATA is None:

        raise HTTPException(
            status_code=503,
            detail=(
                "No feature metadata available"
            )
        )

    sample = {}

    # --------------------------------------------------------
    # GENERATE VALID VALUES
    # --------------------------------------------------------

    for feature_name, metadata in (
        FEATURE_METADATA.items()
    ):

        # ----------------------------------------------------
        # CATEGORICAL
        # ----------------------------------------------------

        if metadata["type"] == "categorical":

            values = metadata["values"]

            if not values:

                sample[feature_name] = ""

            else:

                sample[feature_name] = (
                    random.choice(values)
                )

        # ----------------------------------------------------
        # NUMERIC
        # ----------------------------------------------------

        else:

            min_value = metadata["min"]
            max_value = metadata["max"]

            # safety fallback
            if (
                min_value is None
                or max_value is None
            ):

                sample[feature_name] = 0

            else:

                # integer-like values
                if (
                    float(min_value).is_integer()
                    and float(max_value).is_integer()
                ):

                    sample[feature_name] = (
                        random.randint(
                            int(min_value),
                            int(max_value)
                        )
                    )

                # float values
                else:

                    sample[feature_name] = round(
                        random.uniform(
                            float(min_value),
                            float(max_value)
                        ),
                        4
                    )

    return {
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "model_alias": MODEL_ALIAS,
        "payload": sample
    }