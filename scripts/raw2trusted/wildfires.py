import os
import sys
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, year, month, concat_ws, md5, create_map, to_date
from pyspark.sql.types import StringType, DoubleType, TimestampType, IntegerType
from delta.tables import DeltaTable

# Adiciona a raiz do projeto ao sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.transforms import normalize_df, validate_lat_lon, validate_fire_risk

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
        .appName("WildfiresRaw2Trusted")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "3g") 
        .config("spark.executor.memory", "3g") 
        .config("spark.executor.cores", "4")
        .config("spark.speculation","false")
        .getOrCreate()
)

colunas = ["lat",
           "lon",
           "data_hora_gmt",
           "satelite",
           "municipio",
           "estado",
           "bioma",
           "numero_dias_sem_chuva",
           "precipitacao",
           "risco_fogo",
           "frp"]

daily_df = spark.read.csv(daily_path, header=True, inferSchema=True).select(colunas)
if not DeltaTable.isDeltaTable(spark, final_path):
    monthly_df = spark.read.csv(monthly_path, header=True, inferSchema=True).select(colunas)
    yearly_df = spark.read.csv(yearly_path, header=True, inferSchema=True)
    yearly_df = (
        yearly_df
        .withColumnRenamed("latitude", "lat")
        .withColumnRenamed("longitude", "lon")
        .withColumnRenamed("data_pas", "data_hora_gmt")
    ).select(colunas)

    raw_df = daily_df.unionByName(monthly_df).unionByName(yearly_df).select(colunas)
else:
    raw_df = daily_df

raw_df = raw_df.withColumn(
    'numero_dias_sem_chuva', 
    when(col('numero_dias_sem_chuva') < 0, None).otherwise(col('numero_dias_sem_chuva'))
)

raw_df = raw_df.withColumn(
    'precipitacao',
    when(col('precipitacao') < 0, None).otherwise(col('precipitacao'))
)

raw_df = raw_df.withColumn(
    'risco_fogo',
    when((col('risco_fogo') < 0) | (col('risco_fogo') > 1), None).otherwise(col('risco_fogo'))
)

raw_df = raw_df.withColumn(
    "id",
    md5(
        concat_ws(
            "_",  
            col("lat"),
            col("lon"),
            col("data_hora_gmt"),
            col("satelite"),
            col("municipio"),
            col("estado"),
            col("bioma"),
            col("numero_dias_sem_chuva"),
            col("precipitacao"),
            col("risco_fogo"),
            col("frp")
        )
    )
)

raw_df = raw_df.select(
    col("id").cast(StringType()),
    col("lat").cast(DoubleType()),
    col("lon").cast(DoubleType()),
    col("data_hora_gmt").cast(TimestampType()),
    col("satelite").cast(StringType()),
    col("municipio").cast(StringType()),
    col("estado").cast(StringType()),
    col("bioma").cast(StringType()),
    col("numero_dias_sem_chuva").cast(IntegerType()),
    col("precipitacao").cast(DoubleType()),
    col("risco_fogo").cast(DoubleType()),
    col("frp").cast(DoubleType())
).dropDuplicates()

df_normalized = normalize_df(raw_df)

df_validated = validate_lat_lon(validate_fire_risk(df_normalized, "risco_fogo"), "lat", "lon")

# Dicionário com os mapeamentos de estado para sigla
estado_para_sigla = {
    "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO SUL": "RS",
    "RONDONIA": "RO",
    "MATO GROSSO": "MT",
    "ESPIRITO SANTO": "ES",
    "AMAZONAS": "AM",
    "SERGIPE": "SE",
    "RIO GRANDE DO NORTE": "RN",
    "SANTA CATARINA": "SC",
    "PARAIBA": "PB",
    "GOIAS": "GO",
    "MINAS GERAIS": "MG",
    "SAO PAULO": "SP",
    "PARANA": "PR",
    "ALAGOAS": "AL",
    "MARANHAO": "MA",
    "DISTRITO FEDERAL": "DF",
    "PIAUI": "PI",
    "ACRE": "AC",
    "PARA": "PA",
    "MATO GROSSO DO SUL": "MS",
    "BAHIA": "BA",
    "PERNAMBUCO": "PE",
    "CEARA": "CE",
    "TOCANTINS": "TO",
    "AMAPA": "AP",
    "RORAIMA": "RR"
}
mapping_expr = create_map([item for pair in estado_para_sigla.items() for item in (lit(pair[0]), lit(pair[1]))])

df_ajustado = df_validated.withColumn("sigla_estado", mapping_expr[col("estado")])

df_ajustado = df_ajustado.withColumn("dt_data", to_date(df_ajustado["data_hora_gmt"]))

df_final = (
    df_ajustado
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