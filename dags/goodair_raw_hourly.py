import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="goodair_raw_hourly",   # ✅ tu gardes ce nom si tu veux
    schedule="0 * * * *",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    tags=["goodair"],
) as dag:

    raw_ingest_paris = BashOperator(
        task_id="raw_ingest_paris",
        execution_timeout=timedelta(minutes=30),
        bash_command="""
        docker exec spark-master bash -lc '
          export HADOOP_USER_NAME=hdfs
          /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.network.timeout=600s \
            --conf spark.executor.heartbeatInterval=60s \
            --conf spark.executor.memoryOverhead=512m \
            --conf spark.sql.broadcastTimeout=600 \
            --conf spark.pyspark.python=/usr/bin/python3 \
            --conf spark.pyspark.driver.python=/usr/bin/python3 \
            --conf spark.sql.session.timeZone=Europe/Paris \
            --conf spark.hadoop.fs.defaultFS=hdfs://hadoop:9000 \
            --conf spark.sql.sources.partitionOverwriteMode=dynamic \
            --conf spark.executorEnv.HADOOP_USER_NAME=hdfs \
            --executor-cores 1 \
            --executor-memory 2g \
            /opt/jobs/raw_ingest_paris.py \
            --run-ts "{{ data_interval_end.in_timezone('Europe/Paris') }}"
        '
        """,
    )

    goodair_silver_hourly = BashOperator(
        task_id="goodair_silver_hourly",
        execution_timeout=timedelta(minutes=30),
        bash_command="""
        docker exec spark-master bash -lc '
          export HADOOP_USER_NAME=hdfs
          /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.network.timeout=600s \
            --conf spark.executor.heartbeatInterval=60s \
            --conf spark.executor.memoryOverhead=512m \
            --conf spark.sql.broadcastTimeout=600 \
            --conf spark.pyspark.python=/usr/bin/python3 \
            --conf spark.pyspark.driver.python=/usr/bin/python3 \
            --conf spark.sql.session.timeZone=Europe/Paris \
            --conf spark.hadoop.fs.defaultFS=hdfs://hadoop:9000 \
            --conf spark.sql.sources.partitionOverwriteMode=dynamic \
            --conf spark.executorEnv.HADOOP_USER_NAME=hdfs \
            --total-executor-cores 1 \
            --executor-cores 1 \
            --executor-memory 2g \
            --jars /opt/jars/postgresql-42.7.4.jar \
            /opt/jobs/goodair_silver_hourly.py \
            --dt "{{ data_interval_end.in_timezone('Europe/Paris').strftime('%Y-%m-%d') }}" \
            --hour "{{ data_interval_end.in_timezone('Europe/Paris').strftime('%H') }}"
        '
        """,
    )

    raw_ingest_paris >> goodair_silver_hourly
