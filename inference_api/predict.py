import pandas as pd
import joblib
import pickle
import boto3
from io import BytesIO
from botocore.exceptions import ClientError
import time


# ------------------------------------------------------------------------------
# Columnas esperadas
# ------------------------------------------------------------------------------

NUM_COLS = [
    "Elevation",
    "Aspect",
    "Slope",
    "Horizontal_Distance_To_Hydrology",
    "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am",
    "Hillshade_Noon",
    "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points"
]

CAT_COLS = [
    "Soil_Type",
    "Wilderness_Area"
]

ALL_COLS = NUM_COLS + CAT_COLS


# ------------------------------------------------------------------------------
# Cargar encoder desde volumen
# ------------------------------------------------------------------------------

def load_encoder():

    """
    Carga el encoder OneHotEncoder almacenado en el volumen /encoders.
    """

    ohe = joblib.load("/app/encoders/ohe_encoder.joblib")
    print("loaded encoder")

    return ohe



def safe_load(model_key, bucket="models-bucket"):
    try:
        return load_model_from_minio(model_key, bucket=bucket)

    except ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code == "NoSuchBucket":
            print(f"⚠️ Bucket no encontrado aún → {model_key}")
            return None, None

        elif error_code == "NoSuchKey":
            print(f"⚠️ Modelo no encontrado aún → {model_key}")
            return None, None

        else:
            raise

    except Exception as e:
        print(f"⚠️ Error cargando {model_key}: {e}")
        return None, None

# ------------------------------------------------------------------------------
# Cargar modelo desde MinIO
# ------------------------------------------------------------------------------

def load_model_from_minio(model_key, bucket="models-bucket"):

    """
    Descarga un modelo desde MinIO y lo carga en memoria.

    Parameters
    ----------
    model_key : str
        Ruta del modelo dentro del bucket (ej: models/decision_tree.pkl)

    Returns
    -------
    tuple
        model, scaler
    """

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="supersecret"
    )
    

    response = s3.get_object(
        Bucket=bucket,
        Key=model_key
    )

    model_bytes = response["Body"].read()

    payload = pickle.load(BytesIO(model_bytes))

    model = payload["model"]
    scaler = payload.get("scaler")

    return model, scaler


# ------------------------------------------------------------------------------
# Predicción
# ------------------------------------------------------------------------------

def predict_new_data(df_new, model, scaler=None):

    """
    Ejecuta predicción sobre nuevos datos usando el encoder y el modelo.

    Parameters
    ----------
    df_new : DataFrame
    model : modelo entrenado
    scaler : scaler opcional

    Returns
    -------
    array con predicciones
    """

    df_new = df_new.copy()

    # cargar encoder en cada request
    ohe = load_encoder()


    # ----------------------------
    # limpieza datos
    # ----------------------------

    df_new[NUM_COLS] = df_new[NUM_COLS].fillna(df_new[NUM_COLS].median())

    for col in CAT_COLS:
        df_new[col] = df_new[col].fillna("Unknown")

    # ----------------------------
    # one hot encoding
    # ----------------------------

    X_cat = ohe.transform(df_new[CAT_COLS])

    cat_feature_names = ohe.get_feature_names_out(CAT_COLS)

    X_cat_df = pd.DataFrame(
        X_cat,
        columns=cat_feature_names,
        index=df_new.index
    )

    X_final = pd.concat([df_new[NUM_COLS], X_cat_df], axis=1)

    ordered_cols = list(NUM_COLS) + list(cat_feature_names)

    X_final = X_final[ordered_cols]

    # ----------------------------
    # escalado
    # ----------------------------

    if scaler is not None:

        X_scaled = scaler.transform(X_final)

        X_final = pd.DataFrame(
            X_scaled,
            columns=X_final.columns,
            index=X_final.index
        )

    # ----------------------------
    # predicción
    # ----------------------------

    return model.predict(X_final)