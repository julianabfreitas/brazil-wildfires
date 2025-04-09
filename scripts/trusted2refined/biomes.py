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

path_trusted = "s3a://trusted/biome/map/"
path_final = "s3a://refined/dm_bioma/"

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("BiomesTrusted2Refined")
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

df_biome = spark.read.format("delta").load(path_trusted)

df_final = df_biome.select(
    col("id").alias("id_bioma"),
    col("code_bioma").alias("vl_codigo"),
    col("nome_bioma").alias("des_nome"),
    col("geom").alias("geom_bioma")
)

(
    df_final
    .write
    .mode("overwrite")
    .format("delta")
    .save(path_final)
)