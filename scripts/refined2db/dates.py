import os
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

POSTGIS_USER = os.getenv("POSTGIS_USER")
POSTGIS_PASSWORD = os.getenv("POSTGIS_PASSWORD")
POSTGIS_DB = os.getenv("POSTGIS_DB")

table_name = 'wildfires.dm_data'

path = "s3a://refined/dm_data/"

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("DatesRefined2DB")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

df_data = spark.read.format("delta").load(path)

url = f"jdbc:postgresql://postgis:5432/{POSTGIS_DB}"

properties = {
    "user": f"{POSTGIS_USER}",
    "password": f"{POSTGIS_PASSWORD}",
    "driver": "org.postgresql.Driver"
}

(
    df_data
    .write
    .jdbc(
            url=url,
            table='wildfires.dm_data', 
            mode="overwrite", 
            properties=properties
    )
)