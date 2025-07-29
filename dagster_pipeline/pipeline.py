from dagster import asset, Definitions
import pandas as pd
from minio import Minio
import os
from sqlalchemy import create_engine

# Configuration
MINIO_BUCKET = "data-bucket"
MINIO_FILE = "Non_financial.xlsx"
LOCAL_FILE = "Non_financial.xlsx"

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5435"
POSTGRES_DB = "demo"
POSTGRES_TABLE = "demo"

@asset
def extract_from_minio():
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    try:
        client.stat_object(MINIO_BUCKET, MINIO_FILE)
    except:
        client.fput_object(MINIO_BUCKET, MINIO_FILE, LOCAL_FILE)

    # Download from MinIO
    client.fget_object(MINIO_BUCKET, MINIO_FILE, MINIO_FILE)

    # ✅ Use first row as header, then drop metadata row (row 1 in Excel)
    df = pd.read_excel(MINIO_FILE, header=0)
    df = df.iloc[1:]  # Drop the metadata/info row that mimics column headers

    return df

@asset
def transform_data(extract_from_minio):
    df = extract_from_minio.copy()

    # ✅ Deduplicate column names manually
    def deduplicate_columns(columns):
        seen = {}
        new_cols = []
        for col in columns:
            if col not in seen:
                seen[col] = 0
                new_cols.append(col)
            else:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
        return new_cols

    df.columns = deduplicate_columns(df.columns)

    # ✅ Clean all object-like columns from spaces and convert to numeric if possible
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.replace(" ", "").str.replace(",", "").replace("nan", None)

        try:
            df[col] = pd.to_numeric(df[col], errors="ignore")
        except:
            pass

    # ✅ Drop columns containing "ZERO" in their name
    df = df.drop(columns=[col for col in df.columns if "ZERO" in col], errors="ignore")


    df['calc_col'] = df['S_IN'] - df['S_OUT']

    return df

@asset
def load_to_postgres(transform_data):
    df = transform_data

    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    df.to_sql(POSTGRES_TABLE, engine, if_exists='replace', index=False)
    print(f"✅ Loaded cleaned data to PostgreSQL table: {POSTGRES_TABLE}")
    return POSTGRES_TABLE

# Register pipeline assets
defs = Definitions(
    assets=[extract_from_minio, transform_data, load_to_postgres]
)
