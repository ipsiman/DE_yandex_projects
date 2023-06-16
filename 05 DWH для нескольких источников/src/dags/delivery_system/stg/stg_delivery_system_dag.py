import logging
import requests

import pendulum
from airflow.decorators import dag, task
from delivery_system.stg.couriers_loader import CourierLoader
from delivery_system.stg.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id='delivery_system_stg_dag',
    schedule_interval='@daily',  # Задаем расписание выполнения дага - раз в сутки.
    start_date=pendulum.datetime(2023, 2, 15, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_delivery_system_stg_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_api_connect = requests

    @task(task_id="courier_load")
    def load_couriers():
        rest_loader = CourierLoader(origin_api_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()

    @task(task_id="delivery_load")
    def load_deliveries(ds=None):
        rest_loader = DeliveryLoader(origin_api_connect, dwh_pg_connect, ds, log)
        rest_loader.load_deliveries()

    couriers_load = load_couriers()
    deliveries_load = load_deliveries()

    couriers_load >> deliveries_load


stg_delivery_system_dag = sprint5_delivery_system_stg_dag()
