import logging
import pendulum

from airflow.decorators import dag, task
from delivery_system.dds.dm_couriers_loader import CourierLoader
from delivery_system.dds.dm_ts_delivery_loader import TimestampLoader
from delivery_system.dds.dm_deliveries_loader import DeliveryLoader
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from lib import ConnectionBuilder
from datetime import timedelta

log = logging.getLogger(__name__)


@dag(
    dag_id='delivery_system_dds_dag',
    schedule_interval='@daily',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 2, 15, tz='UTC'),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_delivery_system_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn('PG_WAREHOUSE_CONNECTION')

    @task(task_id='dds_dm_courier_load')
    def load_dm_couriers():
        rest_loader = CourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()

    @task(task_id='dds_dm_ts_delivery_load')
    def load_dm_ts_delivery():
        rest_loader = TimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()

    @task(task_id='dds_dm_delivery_load')
    def load_dm_deliveries():
        rest_loader = DeliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()

    dm_couriers_load = load_dm_couriers()
    dm_ts_delivery_load = load_dm_ts_delivery()
    dm_deliveries_load = load_dm_deliveries()

    wait_stg_dag = ExternalTaskSensor(
        task_id='external_wait_stg_dag',
        external_dag_id='delivery_system_stg_dag'
    )

    wait_stg_dag >> [dm_couriers_load, dm_ts_delivery_load] >> dm_deliveries_load


dds_delivery_system_ranks_dag = sprint5_delivery_system_dds_dag()
