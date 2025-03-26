import os
from minio import Minio
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from ..utils import get_url_content, unzip_upload_to_minio

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
URL_MUNICIPALITIES_MAP = os.getenv("URL_MUNICIPALITIES_MAP")

bucket_name = 'raw'
bucket_path = 'municipalities/map'

minio_client = Minio(
    MINIO_ENDPOINT, 
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY ,
    secure=False 
)

response = get_url_content(URL_MUNICIPALITIES_MAP)

unzip_upload_to_minio(response, minio_client, bucket_name, bucket_path)