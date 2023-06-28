import airflow
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

dag = DAG(
    dag_id='dag_events',
    schedule_interval=None,
    default_args=default_args
)

reload_events = SparkSubmitOperator(
    task_id='reload_events',
    dag=dag,
    application='/scripts/odd_events.py',
    conn_id='yarn_spark',
    application_args=[
        '/user/master/data/geo/events'
        '/user/ipsiman/data/geo/events'
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)

update_dm_events = SparkSubmitOperator(
    task_id='update_dm_events',
    dag=dag,
    application='/scripts/dm_events.py',
    conn_id='yarn_spark',
    application_args=[
        '/user/ipsiman/data/geo/events',
        '/user/ipsiman/analytics'
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores=2,
    executor_memory='2g'
)


reload_events >> update_dm_events
