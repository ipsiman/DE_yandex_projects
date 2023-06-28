import requests

from logging import Logger
from typing import Dict, List

from airflow.models import Variable
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class CourierReaderApi:
    def __init__(self, rq: requests) -> None:
        self.api = rq
        self.url = Variable.get('delivery_api')
        self.api_name = '/couriers'
        self.headers = {
            'X-Nickname': Variable.get('nickname'),
            'X-Cohort': Variable.get('cohort'),
            'X-API-KEY': Variable.get('api-key')
        }

    def get_couriers(self, courier_limit: int, courier_offset: int) -> List[Dict]:
        data = self.api.get(
            url=self.url + self.api_name,
            headers=self.headers,
            params={
                'sort_field': 'id',
                'sort_direction': 'asc',
                'limit': courier_limit,
                'offset': courier_offset}
        ).json()
        return data


class CourierDestRepository:
    def insert_courier(self, conn: Connection, object_value: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(object_value)
                    VALUES (%(object_value)s)
                    ON CONFLICT (object_value) DO NOTHING;
                """,
                {"object_value": object_value},
            )


class CourierLoader:
    def __init__(self, api_origin: requests, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierReaderApi(api_origin)
        self.stg = CourierDestRepository()
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            # Вычитываем объекты пачками до конца, пока запрос не вернет 0.
            last_offset = 0
            i = 1
            while True:
                load_part = self.origin.get_couriers(30, last_offset)
                self.log.info(f"Found {len(load_part)} couriers to load.")
                last_offset += len(load_part)
                if not load_part:
                    self.log.info(f"Quitting. Total load finished on {last_offset}")
                    return

                # Сохраняем объекты в базу dwh.
                self.log.info(f"Iteration {i}. Load {len(load_part)} couriers")
                for courier in load_part:
                    self.stg.insert_courier(conn, json2str(courier))
                i += 1

