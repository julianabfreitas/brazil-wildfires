from pyspark.sql.functions import col, to_timestamp, when, upper, translate
from pyspark.sql.types import StringType
from sedona.spark import ST_Transform

def normalize_string(text):
    acentos = "áàãâäéèêëíìîïóòõôöúùûüçñÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇÑ"
    sem_acentos = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"
    
    return upper(translate(text, acentos, sem_acentos))

def normalize_timestamp(timestamp):
    return to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss')

def normalize_geometry(geometry_column):
    return ST_Transform(geometry_column, 4674)

def normalize_df(df):
    for column, column_type in df.dtypes:
        if column_type == 'string':
            df = df.withColumn(column, normalize_string(col(column)))
        elif column_type == 'timestamp':
            df = df.withColumn(column, normalize_timestamp(col(column)))
        elif column_type == 'geometry': 
            df = df.withColumn(column, normalize_geometry(col(column)))
    return df

def validate_lat_lon(df, lat_col, lon_col):
    df = df.withColumn(
        "valid_lat",
        when((col(lat_col) >= -90) & (col(lat_col) <= 90), True).otherwise(False)
    )
    df = df.withColumn(
        "valid_lon",
        when((col(lon_col) >= -180) & (col(lon_col) <= 180), True).otherwise(False)
    )
    df = df.withColumn(
        "valid_coordinates",
        when((col("valid_lat") == True) & (col("valid_lon") == True), True).otherwise(False)
    )
    return df

def validate_fire_risk(df, fire_risk_col):
    df = df.withColumn(
        "fire_risk_category",
        when(col(fire_risk_col) <= 0.15, "MINIMO") # 0 até 0.15
        .when((col(fire_risk_col) > 0.15) & (col(fire_risk_col) <= 0.4), "BAIXO")
        .when((col(fire_risk_col) > 0.4) & (col(fire_risk_col) <= 0.7), "MEDIO")
        .when((col(fire_risk_col) > 0.7) & (col(fire_risk_col) <= 0.95), "ALTO")
        .when((col(fire_risk_col) > 0.95) & (col(fire_risk_col) <= 1), "CRITICO")
        .otherwise("INVALIDO")
    )
    return df

