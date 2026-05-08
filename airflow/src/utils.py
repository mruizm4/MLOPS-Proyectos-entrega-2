import os
import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
   String
)
import time
import requests
from sklearn.preprocessing import OneHotEncoder
import hashlib



MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql_db"),
    "user": os.getenv("MYSQL_USER", "mlops_user"),
    "password": os.getenv("MYSQL_PASSWORD", "mlops_pass"),
    "database": os.getenv("MYSQL_DB", "mlops_db"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
}


COLUMNS = [
    "Elevation",
    "Aspect",
    "Slope",
    "Horizontal_Distance_To_Hydrology",
    "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am",
    "Hillshade_Noon",
    "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points",
    "Wilderness_Area",
    "Soil_Type",
    "Cover_Type"
]

metadata = MetaData()

def get_data():

    url = "http://get_data_api:8003/data"

    params = {
        "group_number": 3
    }

    for _ in range(10):
        try:
            response = requests.get(url, params=params)
            return response.json()
        except:
            print("Waiting for API...")
            time.sleep(3)


def api_to_dataframe(api_response):
    
    try:
        df = pd.DataFrame(api_response["data"], columns=COLUMNS)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return api_response

    # convertir a numéricos donde corresponde
    numeric_cols = COLUMNS[:10] + ["Cover_Type"]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

    return df

"""
covertype_raw = Table(
    "covertype_raw",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("uuid", String(64), index=True)
)

covertype_processed = Table(
    "covertype_processed",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("uuid", String(64), unique=True, index=True)
)
"""

# ⭐ UUID determinístico basado en contenido de fila
def add_uuid(df):

    def row_hash(row):
        row_str = "|".join(row.astype(str).values)
        return hashlib.sha256(row_str.encode()).hexdigest()

    df = df.copy()
    df["uuid"] = df.apply(row_hash, axis=1)

    return df


def get_engine():
    url = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:"
        f"{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/"
        f"{MYSQL_CONFIG['database']}"
    )
    return create_engine(url)

"""
def wait_for_db(retries=10, sleep=3):

    engine = get_engine()

    for i in range(retries):
        try:
            with engine.connect():
                print("✅ DB ready")
                return
        except Exception as e:
            if i == retries - 1:
                raise RuntimeError(f"Database not reachable: {e}")

            print(f"⏳ Waiting for DB... ({i+1}/{retries})")
            time.sleep(sleep)
"""

def wait_for_db(retries=10, sleep=3):

    engine = get_engine()

    for i in range(retries):
        try:
            with engine.connect():
                print("✅ DB ready")

                # ⭐ crear tablas si no existen
                metadata.create_all(engine)

                return
        except Exception as e:
            if i == retries - 1:
                raise RuntimeError(f"Database not reachable: {e}")

            print(f"⏳ Waiting for DB... ({i+1}/{retries})")
            time.sleep(sleep)


def insert_raw(df):

    engine = get_engine()

    df.to_sql(
        "covertype_raw",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"Inserted RAW rows: {len(df)}")


def get_pending_rows():

    engine = get_engine()

    raw = pd.read_sql_table("covertype_raw", engine)

    try:
        processed = pd.read_sql_table("covertype_processed", engine)
    except:
        print("Processed table does not exist yet")
        return raw

    if processed.empty:
        return raw

    pending = raw.merge(
        processed[["uuid"]],
        on="uuid",
        how="left",
        indicator=True
    )

    pending = pending[pending["_merge"] == "left_only"]

    pending = pending.drop(columns=["_merge"])

    print(f"Pending rows: {len(pending)}")

    return pending

def get_processed_rows():

    engine = get_engine()

    try:
        no_processed = pd.read_sql_table("covertype_raw", engine)
        print(f"Processed rows: {len(no_processed)}")
        return no_processed

    except Exception as e:
        print(f"Error reading processed table: {e}")
        return pd.DataFrame()


def insert_processed(df_processed):

    engine = get_engine()

    df_processed.to_sql(
        "covertype_processed",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"Inserted PROCESSED rows: {len(df_processed)}")




def clear_database(table):

    engine = get_engine()

    metadata.reflect(bind=engine)

    metadata.drop_all(bind=engine , tables=[table])

    print("✅ Tables dropped")



def preprocess_data(df):

    df = df.copy()

    target = "Cover_Type"

    cat_cols = ["Soil_Type", "Wilderness_Area"]

    num_cols = [
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

    # imputación
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())
    df[cat_cols] = df[cat_cols].fillna("Unknown")

    # OneHot
    ohe = OneHotEncoder(
        sparse_output=False,
        handle_unknown="ignore"
    )

    df_cat = ohe.fit_transform(df[cat_cols])

    cat_feature_names = ohe.get_feature_names_out(cat_cols)

    df_cat_df = pd.DataFrame(
        df_cat,
        columns=cat_feature_names,
        index=df.index
    )

    # dataset final
    df_processed = pd.concat(
        [df["uuid"], df[num_cols], df_cat_df, df[target]],
        axis=1
    )

    X = df_processed.drop(columns=[target, "uuid"])

    y = df_processed[target]

    encoders = {"onehot": ohe}

    return df_processed, X, y, encoders

def get_sql_table(table_name):
    engine = get_engine()

    df = pd.read_sql(table_name, con=engine)

    return df

"""
Orden de invocación de las funciones:

if __name__ == "__main__":

    wait_for_db()

#    clear_database()

    api_response = get_data()

    process_api_batch(api_response, "covertype_processed")

# Cómo se usa ahora en el DAG (orden real)

wait_for_db()

api_response = get_data()

df = api_to_dataframe(api_response)

df = add_uuid(df)

insert_raw(df)

pending = get_pending_rows()

if not pending.empty:

    df_processed, X, y, enc = preprocess_data(pending)

    insert_processed(df_processed)
"""
