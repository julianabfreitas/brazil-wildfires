import os
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, year, month, create_map, to_date, collect_list, expr
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.spark import SedonaContext 

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

path_trusted = "s3a://trusted/wildfires/hotspots/"
path_final = "s3a://refined/ft_queimada/"

path_bioma = "s3a://refined//dm_bioma/"
path_data = "s3a://refined//dm_data/"
path_municipio = "s3a://refined//dm_municipio/"

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

spark = (
    SparkSession.builder
        .appName("WildfiresTrusted2Refined")
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
        .config("spark.executor.cores", "3")
        .getOrCreate()
)

sedona = SedonaContext.create(spark) 

df_queimada = spark.read.format("delta").load(path_trusted)
df_bioma = spark.read.format("delta").load(path_bioma)
df_data = spark.read.format("delta").load(path_data)
df_municipio = spark.read.format("delta").load(path_municipio)

df_queimada.createOrReplaceTempView("df_queimada")
df_queimada = spark.sql('''SELECT *, ST_Point(dq.lon, dq.lat) AS geom_foco FROM df_queimada dq WHERE valid_coordinates IS TRUE''')
df_queimada.createOrReplaceTempView("df_queimada")

df_bioma.createOrReplaceTempView("df_bioma")
df_data.createOrReplaceTempView("df_data")
df_municipio.createOrReplaceTempView("df_municipio")

df_ft_queimada = spark.sql('''  SELECT  dq.id AS id_queimada,
                                        dd.id_data,
                                        dm.id_municipio, 
                                        db.id_bioma,
                                        dq.satelite AS des_satelite,
                                        dq.numero_dias_sem_chuva AS qtd_dias_sem_chuva,
                                        dq.precipitacao AS vl_precipitacao,
                                        dq.risco_fogo AS vl_risco_fogo,
                                        dq.frp AS vl_frp,
                                        dq.geom_foco
                                FROM df_queimada dq
                                     LEFT JOIN df_bioma db ON (dq.bioma = db.des_nome)
                                     LEFT JOIN df_municipio dm ON (dq.sigla_estado = dm.des_sigla AND dq.municipio = dm.des_nome)
                                     LEFT JOIN df_data dd ON (dq.dt_data = dd.dt_data) 
                        ''')

(
    df_ft_queimada
    .write
    .mode("overwrite")
    .format("delta")
    .save(path_final)
)

