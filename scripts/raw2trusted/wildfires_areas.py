import os
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, year, month, hash, to_date
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType, DateType
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.spark import SedonaContext
from delta.tables import DeltaTable
from sedona.spark import ST_GeomFromWKT, ST_AsText
from ..transforms import normalize_df, validate_lat_lon, validate_fire_risk

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

raw_path = "s3a://raw//wildfires/areas/monthly/*"
final_path = "s3a://trusted//wildfires/areas"

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

df_spatial = spark.read.format("shapefile").load(raw_path)

df_spatial = df_spatial.withColumn("chave", hash(col("geometry"), col("DN"), col("data")))

df_spatial = df_spatial.select(
    col("chave").cast(IntegerType()).alias("id"),
    to_date(col("data"), "yyyyMMdd").alias("data"),
    col("DN").cast(IntegerType()).alias("dn"),
    ST_GeomFromWKT(col("geometry").cast(StringType())).alias("geom") 
).dropDuplicates()

df_normalized = normalize_df(df_spatial)

df_final = (
    df_normalized
    .withColumn("year", year(col("data")))
    .withColumn("month", month(col("data")))
)

if DeltaTable.isDeltaTable(spark, final_path):
    delta_table = DeltaTable.forPath(spark, final_path)
    
    (
        delta_table.alias("target")
        .merge(
            df_final.alias("source"),
            "target.id = source.id"  
        )
        .whenMatchedUpdateAll()  
        .whenNotMatchedInsertAll() 
        .execute()
    )
else:
    (
        df_final
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .format("delta")
        .save(final_path)
    )

