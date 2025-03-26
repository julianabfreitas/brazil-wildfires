import os
import sys
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.spark import SedonaContext
from sedona.spark import ST_GeomFromWKT

# Adiciona a raiz do projeto ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.transforms import normalize_df

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

path_raw = 'biome/map'
path_final = 'biome/map'

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("ReadCSVMinio")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.serializer", KryoSerializer.getName) 
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)  
        .getOrCreate()
)

sedona = SedonaContext.create(spark) 

df_spatial = spark.read.format("shapefile").load(f"s3a://raw//{path_raw}/")

df_spatial = df_spatial.select(
    col("ID1").cast(IntegerType()).alias("id"),
    col("COD_BIOMA").cast(StringType()).alias("code_bioma"),
    col("NOM_BIOMA").cast(StringType()).alias("nome_bioma"),
    ST_GeomFromWKT(col("geometry").cast(StringType())).alias("geom") 
)

df_final = normalize_df(df_spatial)

(
    df_final
    .write
    .mode("overwrite")
    .format("delta")
    .save(f"s3a://trusted/{path_final}/")
)