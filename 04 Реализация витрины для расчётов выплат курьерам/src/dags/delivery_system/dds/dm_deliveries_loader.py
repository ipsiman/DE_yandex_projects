from datetime import datetime
from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    delivery_ts_id: int
    order_id: int
    courier_id: int
    rate: int
    sum: float
    tip_sum: float


class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    select dd.id,
                           object_value::json ->> 'delivery_id' as delivery_id,
                           dt.id as delivery_ts_id,
                           do2.id as order_id,
                           dc.id as courier_id,
                           object_value::json ->> 'rate' as rate,
                           object_value::json ->> 'sum' as sum,
                           object_value::json ->> 'tip_sum' as tip_sum
                    from stg.deliverysystem_deliveries dd
                    left join dds.dm_timestamps dt on (dd.object_value::json ->> 'order_ts')::timestamp = dt.ts
                    left join dds.dm_orders do2 on dd.object_value::json ->> 'order_id' = do2.order_key
                    left join dds.dm_couriers dc on dd.object_value::json ->> 'courier_id' = dc.courier_id
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY dd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )

            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, event: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries (delivery_id, delivery_ts_id, order_id, courier_id, rate, sum, tip_sum)
                    VALUES
                      (%(delivery_id)s, %(delivery_ts_id)s, %(order_id)s, %(courier_id)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO NOTHING;
                """,
                {
                    "delivery_id": event.delivery_id,
                    "delivery_ts_id": event.delivery_ts_id,
                    "order_id": event.order_id,
                    "courier_id": event.courier_id,
                    "rate": event.rate,
                    "sum": event.sum,
                    "tip_sum": event.tip_sum,
                },
            )


class DeliveryLoader:
    WF_KEY = "dm_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.dds = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки. Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.dds.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
