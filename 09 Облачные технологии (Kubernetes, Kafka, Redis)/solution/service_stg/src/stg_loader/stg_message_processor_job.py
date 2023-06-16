import json

from datetime import datetime
from logging import Logger
from typing import List, Dict

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            if msg['object_type'] == 'order':
                order = msg['payload']
                self._stg_repository.order_events_insert(
                    msg['object_id'],
                    msg['object_type'],
                    msg['sent_dttm'],
                    json.dumps(order))

                user_id = order['user']['id']
                user = self._redis.get(user_id)
                user_name = user["name"]
                user_login = user["login"]

                restaurant_id = order['restaurant']['id']
                restaurant = self._redis.get(restaurant_id)
                restaurant_name = restaurant["name"]

                out_msg = {
                    "object_id": msg['object_id'],
                    "object_type": msg['object_type'],
                    "payload": {
                        "id": msg['object_id'],
                        "date": order['date'],
                        "cost": order["cost"],
                        "payment": order["payment"],
                        "final_status": order["final_status"],
                        "restaurant": {
                            "id": restaurant_id,
                            "name": restaurant_name
                        },
                        "user": {
                            "id": user_id,
                            "name": user_name,
                            "login": user_login
                        },
                        "products": self._format_items(order["order_items"], restaurant)
                    }
                }
                self._producer.produce(out_msg)

                self._logger.info(f"{datetime.utcnow()}: Message sent")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items
