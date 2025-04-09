import os
import psycopg2
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.spark import SedonaContext
from pyspark.sql.functions import expr

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

POSTGIS_USER = os.getenv("POSTGIS_USER")
POSTGIS_PASSWORD = os.getenv("POSTGIS_PASSWORD")
POSTGIS_DB = os.getenv("POSTGIS_DB")
POSTGIS_HOST = "postgis"
POSTGIS_PORT = 5432

url_psycopg2 = f"postgresql://{POSTGIS_USER}:{POSTGIS_PASSWORD}@{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
url_jdbc = f"jdbc:postgresql://{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"

table_name = 'wildfires.dm_municipio'
geo_col = 'geom_municipio'
srid = 4674

create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
        	id_municipio INTEGER,
        	des_nome VARCHAR,
        	des_sigla VARCHAR,
        	vl_area FLOAT8,
        	{geo_col} GEOMETRY
);
'''


path = "s3a://refined/dm_municipio/"

try:
    connection = psycopg2.connect(url_psycopg2)
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        connection.commit()
except Exception as e:
    print(f"Erro ao conectar ou executar comando: {e}")
finally:
    if connection:
        connection.close()

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("MunicipalitiesRefined2DB")
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

df = spark.read.format("delta").load(path)

(
    df
        .withColumn(geo_col, expr(f"ST_AsEWKB(ST_SetSRID({geo_col}, {srid}))"))
        .write.format("jdbc")
        .option("url", url_jdbc)
        .option("dbtable", table_name)
        .option("user", POSTGIS_USER)
        .option("password", POSTGIS_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
)