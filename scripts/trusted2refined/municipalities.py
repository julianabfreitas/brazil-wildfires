import os
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.spark import SedonaContext

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

path_trusted = "s3a://trusted/municipalities/map/"
path_final = "s3a://refined/dm_municipio/"

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("MunicipalitiesTrusted2Refined")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.serializer", KryoSerializer.getName) 
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)  
        .config("spark.driver.memory", "3g") 
        .config("spark.executor.memory", "3g") 
        .config("spark.executor.cores", "4")
        .getOrCreate()
)

sedona = SedonaContext.create(spark) 

df_municipio = spark.read.format("delta").load(path_trusted)

df_final = df_municipio.select(
    col("codigo_municipio").alias("id_municipio"),
    col("nome_municipio").alias("des_nome"),
    col("sigla_uf").alias("des_sigla"),
    col("area_km2").alias("vl_area"),
    col("geom").alias("geom_municipio")
)

(
    df_final
    .write
    .mode("overwrite")
    .format("delta")
    .save(path_final)
)