import logging
import pendulum
import vertica_python

from airflow.decorators import dag, task
from airflow.models import Variable


VERTICA_HOST = Variable.get('VERTICA_HOST')
VERTICA_USER = Variable.get('VERTICA_USER')
VERTICA_PASSWORD = Variable.get('VERTICA_PASSWORD')

log = logging.getLogger(__name__)

vertica_conn_info = {'host': VERTICA_HOST,
                     'port': 5433,
                     'user': VERTICA_USER,
                     'password': VERTICA_PASSWORD,
                     'database': 'dwh',
                     'autocommit': True
                     }


def delete_datamart_data(update_date) -> None:
    delete_query = (
        '''
            DELETE FROM USERYANDEXRU__DWH.global_metrics
            WHERE date_update::date = (%s)
        '''
    )
    try:
        with vertica_python.connect(**vertica_conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(delete_query, (update_date, ))
    except Exception as e:
        log.error('An error occurred during delete datamart data: %s', str(e))
        raise


def upload_datamart_data(update_date, load_date) -> None:
    file = open('/lessons/sql/dml_update_datamart.sql', 'r')
    upload_query = file.read()
    file.close()
    try:
        with vertica_python.connect(**vertica_conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(upload_query, (update_date, load_date, ))
    except Exception as e:
        log.error('An error occurred during upload datamart data: %s', str(e))
        raise


@dag(
    dag_id='datamart_update',
    start_date=pendulum.datetime(2022, 10, 1, tz='UTC'),
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['upload', 'datamart', 'update']
)
def datamart_update_dag():
    @task(task_id='update_data')
    def update_datamart(yesterday_ds=None, ds=None):
        update_date = pendulum.from_format(ds, 'YYYY-MM-DD').to_date_string()
        load_date = pendulum.from_format(yesterday_ds, 'YYYY-MM-DD').to_date_string()

        delete_datamart_data(update_date)
        upload_datamart_data(update_date, load_date)

    update_datamart()


_ = datamart_update_dag()
