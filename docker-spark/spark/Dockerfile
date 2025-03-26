# Usando a imagem Bitnami Spark como base
FROM bitnami/spark:3.5.5-debian-12-r0

USER root
RUN apt-get update && apt-get install -y procps

USER 1001

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Expondo as portas padrão do Spark
EXPOSE 7077 8080 8081

# Dependências MinIO
COPY jars/aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars/
COPY jars/hadoop-aws-3.3.4.jar ${SPARK_HOME}/jars/

# Dependências Delta
COPY jars/delta-spark_2.12-3.2.0.jar ${SPARK_HOME}/jars/
COPY jars/delta-storage-3.2.0.jar ${SPARK_HOME}/jars/

# Dependências Sedona
COPY jars/sedona-spark-shaded-3.5_2.12-1.7.0.jar ${SPARK_HOME}/jars/
COPY jars/geotools-wrapper-1.7.0-28.5.jar ${SPARK_HOME}/jars/

# Dependências PostgreSQL
COPY jars/postgresql-42.7.5.jar ${SPARK_HOME}/jars/

# Configurando o spark-defaults.conf para carregar os JARs automaticamente
# RUN echo "spark.jars ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar,${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar,${SPARK_HOME}/jars/delta-spark_2.12-3.2.0.jar,${SPARK_HOME}/jars/delta-storage-3.2.0.jar,${SPARK_HOME}/jars/sedona-spark-shaded-3.5_2.12-1.7.0.jar,${SPARK_HOME}/jars/geotools-wrapper-1.7.0-28.5.jar,${SPARK_HOME}/jars/postgresql-42.7.5.jar" >> ${SPARK_HOME}/conf/spark-defaults.conf



