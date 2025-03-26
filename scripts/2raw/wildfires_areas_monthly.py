import os
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO
from pyspark.sql import SparkSession
from ..utils import get_url_content, get_links_by_extension, upload_file_to_minio, update_date_control, unzip_upload_to_minio

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
URL_WILDFIRES_AREAS_MONTHLY = os.getenv("URL_WILDFIRES_AREAS_MONTHLY")

bucket_name = 'raw'
bucket_path_monthly = 'wildfires/areas/monthly'
bucket_path_date_control = 'wildfires/areas/date_control.json'

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

response_wildfires_areas_monthly = get_url_content(URL_WILDFIRES_AREAS_MONTHLY)
links_wildfires_areas_monthly_zips = get_links_by_extension(URL_WILDFIRES_AREAS_MONTHLY, response_wildfires_areas_monthly, '.zip')

for link in links_wildfires_areas_monthly_zips:
    link_content = get_url_content(link)
    month = link.split('shp/')[-1][:10].replace('_', '-')
    unzip_upload_to_minio(link_content, minio_client, bucket_name, f'{bucket_path_monthly}/{month}')