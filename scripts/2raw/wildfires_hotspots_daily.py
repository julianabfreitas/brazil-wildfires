import os
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO
from pyspark.sql import SparkSession
from ..utils import get_url_content, get_links_by_extension, upload_file_to_minio, get_max_value, update_date_control

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
URL_WILDFIRES_DAILY = os.getenv("URL_WILDFIRES_DAILY")

bucket_name = 'raw'
bucket_path_daily = 'wildfires/hotspots/daily'
bucket_path_date_control = 'wildfires/hotspots/date_control.json'

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
        .getOrCreate()
)

json_log = spark.read.json(f"s3a://{bucket_name}//{bucket_path_date_control}", multiLine=True)

last_date = json_log.select('last_date').take(1)[0][0]
last_date_formated = int(datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d'))

response_wildfires_daily = get_url_content(URL_WILDFIRES_DAILY)
links_wildfires_daily = get_links_by_extension(URL_WILDFIRES_DAILY, response_wildfires_daily, '.csv')

filtered_links = [
    link for link in links_wildfires_daily 
    if int(link.split('br_')[-1][:8]) > last_date_formated
]

for link in filtered_links:
    file_name = link.split('Brasil/')[-1]
    link_content = get_url_content(link)
    upload_file_to_minio(BytesIO(link_content), len(link_content), minio_client, bucket_name, f"{bucket_path_daily}/{file_name}")

df_wildfires_daily = spark.read.csv(f"s3a://{bucket_name}//{bucket_path_daily}/*", header=True, inferSchema=True)

update_date_control(get_max_value(df_wildfires_daily, 'data_hora_gmt'), minio_client, bucket_name, bucket_path_date_control)

