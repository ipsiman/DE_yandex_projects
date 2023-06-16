import logging
import pendulum
import vertica_python
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.models import Variable

VERTICA_HOST = Variable.get('VERTICA_HOST')
VERTICA_USER = Variable.get('VERTICA_USER')
VERTICA_PASSWORD = Variable.get('VERTICA_PASSWORD')

log = logging.getLogger(__name__)

postgres_hook = PostgresHook('postgresql_conn')
engine = postgres_hook.get_sqlalchemy_engine()

vertica_conn_info = {'host': VERTICA_HOST,
                     'port': 5433,
                     'user': VERTICA_USER,
                     'password': VERTICA_PASSWORD,
                     'database': 'dwh',
                     'autocommit': True
                     }


def load_from_pg(pg_query, params=None) -> pd.DataFrame:
    with engine.connect() as pg_conn:
        return pd.read_sql_query(sql=pg_query, con=pg_conn, params=params)


def upload_to_vertica(query, buffer) -> None:
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.copy(query, buffer)


def get_currencies() -> pd.DataFrame:
    return load_from_pg('SELECT * FROM public.currencies')


def upload_currencies(currencies_df) -> None:
    vert_query = (
        '''
        TRUNCATE TABLE USERYANDEXRU__STAGING.currencies;

        COPY USERYANDEXRU__STAGING.currencies
        (date_update, currency_code, currency_code_with, currency_with_div)
        FROM STDIN
        DELIMITER '|';
        '''
    )
    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.copy(vert_query, currencies_df.to_csv(sep='|', index=False, header=False))


def get_transactions(load_date) -> pd.DataFrame:
    query = (
        '''
            SELECT *
            FROM public.transactions
            WHERE transaction_dt::date = %(load_date)s;
        '''
    )
    return load_from_pg(query, {'load_date': load_date})


def upload_transactions(transactions_df, load_date) -> None:
    vert_delete_query = (
        '''
            DELETE FROM USERYANDEXRU__STAGING.transactions
            WHERE transaction_dt::date = (%s)
        '''
    )
    vert_update_query = (
        '''
            COPY USERYANDEXRU__STAGING.transactions (
                operation_id,
                account_number_from,
                account_number_to,
                currency_code,
                country,
                status,
                transaction_type,
                amount,
                transaction_dt
            )
            FROM STDIN
            DELIMITER '|'
        '''
    )

    with vertica_python.connect(**vertica_conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(vert_delete_query, (load_date,))
            cur.copy(vert_update_query,
                     transactions_df.to_csv(sep='|', index=False, header=False)
                     )


@dag(
    dag_id='upload_to_stg_dag',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['upload', 'stg']
)
def upload_to_stg_dag():
    @task(task_id='upload_currencies')
    def update_currencies():
        currencies_df = get_currencies()
        upload_currencies(currencies_df)

    @task(task_id='upload_transactions')
    def update_transactions(yesterday_ds=None):
        load_date = pendulum.from_format(yesterday_ds, 'YYYY-MM-DD').to_date_string()
        transactions_df = get_transactions(load_date)
        upload_transactions(transactions_df, load_date)

    update_currencies() >> update_transactions()


_ = upload_to_stg_dag()
