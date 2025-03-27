from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'juliana.bernardes',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='dates_data_pipeline',
    default_args=default_args,
    catchup=False,
    tags=['daily']
) as dag:

    # Tarefa 1: Refinamento dos dados
    refine_task = SparkSubmitOperator(
        task_id='refine_data',
        application='/opt/airflow/scripts/trusted2refined/dates.py',
        conn_id='spark_default',
        verbose=True,
        jars='/usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/delta-spark_2.12-3.2.0.jar,/usr/local/spark/jars/delta-storage-3.2.0.jar'
    )

    # Tarefa 2: Carregar os dados no banco de dados
    load_task = SparkSubmitOperator(
        task_id='load_to_db',
        application='/opt/airflow/scripts/refined2db/dates.py',
        conn_id='spark_default',
        verbose=True,
        jars='/usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/usr/local/spark/jars/hadoop-aws-3.3.4.jar,/usr/local/spark/jars/delta-spark_2.12-3.2.0.jar,/usr/local/spark/jars/delta-storage-3.2.0.jar,/usr/local/spark/jars/postgresql-42.7.5.jar'
    )

    # Definição da ordem de execução das tarefas
    refine_task >> load_task