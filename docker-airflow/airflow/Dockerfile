# airflow/Dockerfile
FROM apache/airflow:2.10.4

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

RUN apt-get install -y procps

# Voltar para o usuário airflow
USER airflow

# Instalar dependências do Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Dependências Hadoop (MinIo)
# COPY ../jars/hadoop-aws-3.3.4.jar /usr/local/spark/jars/
# COPY ../jars/aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars/

