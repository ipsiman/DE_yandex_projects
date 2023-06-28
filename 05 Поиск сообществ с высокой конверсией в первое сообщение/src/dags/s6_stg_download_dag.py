import boto3
import logging
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')

session = boto3.session.Session()
s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def fetch_s3_file(bucket: str, key: str):
    log.info(f'Start download {key}')
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )


@dag(
    dag_id='s6_download_files_from_s3_dag',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule_interval=None,
    tags=['sprint6', 'download']
)
def s6_download_files_dag():
    bucket = 'sprint6'
    bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']

    @task(task_id='fetch_s3_file')
    def load_files():
        for f in bucket_files:
            fetch_s3_file(bucket, f)

    test_print_files = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command='head -n 10 {{ params.files }}',
        params={'files': ' '.join([f'/data/{f}' for f in bucket_files])}
    )

    load_files = load_files()

    load_files >> test_print_files


_ = s6_download_files_dag()
