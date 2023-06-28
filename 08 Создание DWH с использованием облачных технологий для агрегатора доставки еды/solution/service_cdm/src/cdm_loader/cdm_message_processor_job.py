from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from cdm_loader.repository import OrderCdmBuilder, CdmRepository, OrderObj


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            if msg['object_type'] == 'order' and msg['payload']['final_status'] == 'CLOSED':
                order = OrderObj
                payload = msg['payload']
                order.id = payload['id']
                order.date = payload['date']
                order.final_status = payload['final_status']
                order.user = payload['user_id']
                order.products = payload['products']

                builder = OrderCdmBuilder(order)

                self._logger.info(f"{datetime.utcnow()}: Start update tables")
                self._update_tables(builder)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _update_tables(self, builder: OrderCdmBuilder) -> None:
        for uc in builder.user_category_count():
            self._cdm_repository.user_category_count_insert(uc)
        for up in builder.user_product_count():
            self._cdm_repository.user_product_count_insert(up)
