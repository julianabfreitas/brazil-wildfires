import os
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, year, month
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType
from transforms import normalize_df, validate_lat_lon, validate_fire_risk
from delta.tables import DeltaTable

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

daily_path = "s3a://raw//wildfires/hotspots/daily/*"
monthly_path = "s3a://raw//wildfires/hotspots/monthly/*"
yearly_path = "s3a://raw//wildfires/hotspots/yearly/*"
final_path = "s3a://trusted//wildfires/hotspots"

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
        .getOrCreate()
)

daily_df = spark.read.csv(daily_path, header=True, inferSchema=True)
if DeltaTable.isDeltaTable(spark, final_path):
    monthly_df = spark.read.csv(monthly_path, header=True, inferSchema=True)
    yearly_df = spark.read.csv(yearly_path, header=True, inferSchema=True)
    yearly_df = (
        yearly_df
        .withColumnRenamed("foco_id", "id")
        .withColumnRenamed("data_pas", "data_hora_gmt")
    )

    raw_df = daily_df.unionByName(monthly_df).unionByName(yearly_df, allowMissingColumns=True)
else:
    raw_df = daily_df

raw_df = raw_df.select(
    col("id").cast(StringType()),
    col("lat").cast(DoubleType()),
    col("lon").cast(DoubleType()),
    col("data_hora_gmt").cast(TimestampType()),
    col("satelite").cast(StringType()),
    col("municipio").cast(StringType()),
    col("estado").cast(StringType()),
    col("pais").cast(StringType()),
    col("municipio_id").cast(IntegerType()),
    col("estado_id").cast(IntegerType()),
    col("pais_id").cast(IntegerType()),
    col("numero_dias_sem_chuva").cast(DoubleType()),
    col("precipitacao").cast(DoubleType()),
    col("risco_fogo").cast(DoubleType()),
    col("bioma").cast(StringType()),
    col("frp").cast(DoubleType())
)

df_normalized = normalize_df(raw_df)

df_validated = validate_lat_lon(validate_fire_risk(df_normalized, "risco_fogo"), "lat", "lon")

df_final = (
    df_validated
    .withColumn("year", year(col("data_hora_gmt")))
    .withColumn("month", month(col("data_hora_gmt")))
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

