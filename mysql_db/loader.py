import os
import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    Float
)
import time
import requests
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

def  get_data():

    url = "http://fastapi:8003/data"

    params = {
        "group_number": 3
    }

    response = requests.get(url, params=params)

    return response.json()



def api_to_dataframe(api_response):

    df = pd.DataFrame(api_response["data"], columns=COLUMNS)

    # convertir a numéricos donde corresponde
    numeric_cols = COLUMNS[:10] + ["Cover_Type"]

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

    return df

covertype_raw = Table(
    "covertype_raw",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True)
)

def get_engine():
    url = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:"
        f"{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/"
        f"{MYSQL_CONFIG['database']}"
    )
    return create_engine(url)


# ⭐ UUID determinístico basado en contenido de fila
def add_uuid(df):

    def row_hash(row):
        row_str = "|".join(row.astype(str).values)
        return hashlib.sha256(row_str.encode()).hexdigest()

    df = df.copy()
    df["uuid"] = df.apply(row_hash, axis=1)

    return df

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

def insert_batch(df, engine):

    df.to_sql(
        "covertype_raw",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"Inserted {len(df)} rows")

"""
def process_api_batch(api_response):

    engine = get_engine()

    df = api_to_dataframe(api_response)

    insert_batch(df, engine)
"""

def process_api_batch(df):

    engine = get_engine()

    insert_batch(df, engine)

def clear_database():

    engine = get_engine()

    metadata.reflect(bind=engine)

    metadata.drop_all(bind=engine , tables=[covertype_raw])

    print("✅ Tables dropped")


if __name__ == "__main__":

    wait_for_db()

    clear_database()

    df = api_to_dataframe(get_data())
    df = add_uuid(df)

    process_api_batch(df)

    