import os
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, year, month, explode, max, year, month, dayofmonth, hash
from pyspark.sql.types import IntegerType, DateType
from delta.tables import DeltaTable
import datetime

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

min_date = datetime.date(2000, 1, 1)
current_date = datetime.date.today()

path_final = "s3a://refined/dm_data"

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("DatesTrusted2Refined")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .getOrCreate()
)

if DeltaTable.isDeltaTable(spark, path_final):
    df_data = DeltaTable.forPath(spark, path_final).toDF()
    
    last_date = df_data.select(max(col("dt_data")).alias("ultima_data")).collect()[0]["ultima_data"]
    #last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d").date()
else:
    df_data = None
    last_date = min_date - datetime.timedelta(days=1)

start_date = last_date + datetime.timedelta(days=1)

if start_date <= current_date:
    df_new = spark.sql(
        f"SELECT sequence(to_date('{start_date}'), to_date('{current_date}'), interval 1 day) as data"
    )
    df_new = df_new.select(explode(col("data")).alias("dt_data"))

    df_new = (
        df_new
        .withColumn("id_data", hash(col("dt_data")))
        .withColumn("qtd_dia", dayofmonth(col("dt_data")))
        .withColumn("qtd_mes", month(col("dt_data")))
        .withColumn("qtd_ano", year(col("dt_data")))
    )
    
    df_final = df_new.select(
        col("id_data").cast(IntegerType()),
        col("qtd_dia").cast(IntegerType()),
        col("qtd_mes").cast(IntegerType()),
        col("qtd_ano").cast(IntegerType()),
        col("dt_data").cast(DateType())
    )

else:
    df_final = None

if DeltaTable.isDeltaTable(spark, path_final):
    delta_table = DeltaTable.forPath(spark, path_final)
    
    (
        delta_table.alias("target")
        .merge(
            df_final.alias("source"),
            "target.id_data = source.id_data"  
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
        .format("delta")
        .save(path_final)
    )