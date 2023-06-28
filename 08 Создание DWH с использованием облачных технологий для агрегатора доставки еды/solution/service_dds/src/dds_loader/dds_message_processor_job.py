from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import OrderDdsBuilder, DdsRepository, OrderObj


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            if msg['object_type'] == 'order':
                order = OrderObj
                payload = msg['payload']
                order.id = payload['id']
                order.date = payload['date']
                order.cost = payload['cost']
                order.payment = payload['payment']
                order.final_status = payload['final_status']
                order.restaurant = payload['restaurant']
                order.user = payload['user']
                order.products = payload['products']

                out_msg = {
                    "object_id": msg['object_id'],
                    "object_type": msg['object_type'],
                    "payload": {
                        "id": order.id,
                        "date": order.date,
                        "final_status": order.final_status,
                        "user_id": order.user['id'],
                        "products": order.products
                    }
                }
                self._producer.produce(out_msg)
                self._logger.info(f"{datetime.utcnow()}: Message sent")

                builder = OrderDdsBuilder(order)

                self._logger.info(f"{datetime.utcnow()}: Start update hub tables")
                self._load_hubs(builder)

                self._logger.info(f"{datetime.utcnow()}: Start update link tables")
                self._load_links(builder)

                self._logger.info(f"{datetime.utcnow()}: Start update satellite tables")
                self._load_sats(builder)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _load_hubs(self, builder: OrderDdsBuilder) -> None:
        self._dds_repository.h_order_insert(builder.h_order())
        self._dds_repository.h_user_insert(builder.h_user())
        self._dds_repository.h_restaurant_insert(builder.h_restaurant())
        for p in builder.h_product():
            self._dds_repository.h_product_insert(p)
        for c in builder.h_category():
            self._dds_repository.h_category_insert(c)

    def _load_links(self, builder: OrderDdsBuilder) -> None:
        self._dds_repository.l_order_user_insert(builder.l_order_user())
        for op in builder.l_order_product():
            self._dds_repository.l_order_product_insert(op)
        for pc in builder.l_product_category():
            self._dds_repository.l_product_category_insert(pc)
        for pr in builder.l_product_restaurant():
            self._dds_repository.l_product_restaurant_insert(pr)

    def _load_sats(self, builder: OrderDdsBuilder) -> None:
        self._dds_repository.s_order_cost_insert(builder.s_order_cost())
        self._dds_repository.s_order_status_insert(builder.s_order_status())
        self._dds_repository.s_restaurant_names_insert(builder.s_restaurant_names())
        self._dds_repository.s_user_names_insert(builder.s_user_names())
        for pn in builder.s_product_names():
            self._dds_repository.s_product_names_insert(pn)
