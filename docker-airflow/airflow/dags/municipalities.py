from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'juliana.bernardes',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='municipalities_data_pipeline',
    default_args=default_args,
    catchup=False,
    tags=['batch']
) as dag:

    # Tarefa 1: Ingestão de dados
    ingest_task = SparkSubmitOperator(
        task_id='ingest_data',
        application='/opt/airflow/scripts/2raw/municipalities_map.py',
        conn_id='spark_default',
        verbose=True
    )

    # Tarefa 2: Processamento inicial dos dados
    process_task = SparkSubmitOperator(
        task_id='process_data',
        application='/opt/airflow/scripts/raw2trusted/municipalities.py',
        conn_id='spark_default',
        verbose=True,
        jars='/usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/delta-spark_2.12-3.2.0.jar,/usr/local/spark/jars/delta-storage-3.2.0.jar,/usr/local/spark/jars/sedona-spark-shaded-3.5_2.12-1.7.0.jar,/usr/local/spark/jars/geotools-wrapper-1.7.0-28.5.jar'
    )

    # Tarefa 3: Refinamento dos dados
    refine_task = SparkSubmitOperator(
        task_id='refine_data',
        application='/opt/airflow/scripts/trusted2refined/municipalities.py',
        conn_id='spark_default',
        verbose=True,
        jars='/usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/delta-spark_2.12-3.2.0.jar,/usr/local/spark/jars/delta-storage-3.2.0.jar,/usr/local/spark/jars/sedona-spark-shaded-3.5_2.12-1.7.0.jar,/usr/local/spark/jars/geotools-wrapper-1.7.0-28.5.jar'
    )

    # Tarefa 4: Carregar os dados no banco de dados
    load_task = SparkSubmitOperator(
        task_id='load_to_db',
        application='/opt/airflow/scripts/refined2db/municipalities.py',
        conn_id='spark_default',
        verbose=True,
        jars='/usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/delta-spark_2.12-3.2.0.jar,/usr/local/spark/jars/delta-storage-3.2.0.jar,/usr/local/spark/jars/sedona-spark-shaded-3.5_2.12-1.7.0.jar,/usr/local/spark/jars/geotools-wrapper-1.7.0-28.5.jar,/usr/local/spark/jars/postgresql-42.7.5.jar'
    )

    # Definição da ordem de execução das tarefas
    ingest_task >> process_task >> refine_task >> load_task