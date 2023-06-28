import logging
import pendulum

from airflow.decorators import dag, task
from lib import ConnectionBuilder, PgConnect
from psycopg import Connection

log = logging.getLogger(__name__)


def insert_update(conn: Connection, f_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(open(f'/lessons/dags/delivery_system/cdm/{f_name}', 'r').read())


@dag(
    schedule_interval='0/60 * * * *',  # Задаем расписание выполнения дага - каждый 60 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_cdm_courier_ledger_update_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

    @task(task_id='dm_courier_ledger_update')
    def dm_courier_ledger_update(pg_dest: PgConnect):
        f_name = 'dm_courier_ledger_update_dml.sql'
        with pg_dest.connection() as conn:
            insert_update(conn, f_name)

    update_dm_courier_ledger = dm_courier_ledger_update(dwh_pg_connect)

    update_dm_courier_ledger


update_sprint5_cdm_courier_ledger_dag = sprint5_cdm_courier_ledger_update_dag()
