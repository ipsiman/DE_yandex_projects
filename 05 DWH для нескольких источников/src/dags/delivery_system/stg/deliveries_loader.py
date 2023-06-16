import logging

import requests

from logging import Logger
from typing import Dict, List

from datetime import datetime, timedelta
from airflow.models import Variable
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class DeliveryReaderApi:
    def __init__(self, rq: requests) -> None:
        self.api = rq
        self.url = Variable.get('delivery_api')
        self.api_name = '/deliveries'
        self.headers = {
            'X-Nickname': Variable.get('nickname'),
            'X-Cohort': Variable.get('cohort'),
            'X-API-KEY': Variable.get('api-key')
        }

    def get_deliveries(self, limit: int, offset: int, start_date: str, end_date: str) -> List[Dict]:
        data = self.api.get(
            url=self.url + self.api_name,
            headers=self.headers,
            params={
                'sort_field': 'id',
                'sort_direction': 'asc',
                'from': start_date,
                'to': end_date,
                'limit': limit,
                'offset': offset}
        ).json()
        return data


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, object_value: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(object_value)
                    VALUES (%(object_value)s)
                    ON CONFLICT (object_value) DO NOTHING;
                """,
                {"object_value": object_value},
            )


class DeliveryLoader:
    BATCH_LIMIT = 50

    def __init__(self, api_origin: requests, pg_dest: PgConnect, date: str, log: Logger) -> None:
        self.start_date = str(datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1))
        self.end_date = str(datetime.strptime(date, '%Y-%m-%d'))
        self.pg_dest = pg_dest
        self.origin = DeliveryReaderApi(api_origin)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            offset = 0
            while True:
                self.log.info(f'Запрос доставок: {self.BATCH_LIMIT}, {offset}, {self.start_date}, {self.end_date}')
                load_queue = self.origin.get_deliveries(self.BATCH_LIMIT, offset, self.start_date, self.end_date)
                self.log.info(f'Found {len(load_queue)} deliveries to load.')
                if not load_queue:
                    self.log.info(f'Quitting. Load finished on {offset}')
                    return

                # Сохраняем объекты в базу dwh.
                for delivery in load_queue:
                    self.stg.insert_delivery(conn, json2str(delivery))
                offset += len(load_queue)
