import uuid

from datetime import datetime
from typing import Any, Dict, List
from lib.pg import PgConnect
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    date: datetime
    final_status: str
    user: str
    products: List[Dict]


class UserCategoryCount(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int


class UserProductCount(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int


class OrderCdmBuilder:
    def __init__(self, order: OrderObj) -> None:
        self._order = order
        self.source_system = 'orders-system-kafka'

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=uuid.NAMESPACE_DNS, name=str(obj))

    def user_category_count(self) -> List[UserCategoryCount]:
        categories = []

        for it in self._order.products:
            categories.append(
                UserCategoryCount(
                    user_id=self._uuid(self._order.user),
                    category_id=self._uuid(it['category']),
                    category_name=it['category'],
                    order_cnt=1
                )
            )
        return categories

    def user_product_count(self) -> List[UserProductCount]:
        products = []

        for it in self._order.products:
            products.append(
                UserProductCount(
                    user_id=self._uuid(self._order.user),
                    product_id=self._uuid(it['id']),
                    product_name=it['name'],
                    order_cnt=1
                )
            )
        return products


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self.source_system = "orders-system-kafka"

    def user_category_count_insert(self, user_category: UserCategoryCount) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (
                            user_id,
                            category_id,
                            category_name,
                            order_cnt
                        )
                        VALUES (
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            %(order_cnt)s
                        )
                        on conflict (user_id, category_id) DO UPDATE SET
                            id=EXCLUDED.id,
                            category_name=EXCLUDED.category_name,
                            order_cnt=EXCLUDED.order_cnt + 1;
                    """,
                    {
                        'user_id': user_category.user_id,
                        'category_id': user_category.category_id,
                        'category_name': user_category.category_name,
                        'order_cnt': user_category.order_cnt
                    }
                )

    def user_product_count_insert(self, user_product: UserProductCount) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (
                            user_id,
                            product_id,
                            product_name,
                            order_cnt
                        )
                        VALUES (
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            %(order_cnt)s
                        )
                        on conflict (user_id, product_id) DO UPDATE SET
                            id=EXCLUDED.id,
                            product_name=EXCLUDED.product_name,
                            order_cnt=EXCLUDED.order_cnt + 1;
                    """,
                    {
                        'user_id': user_product.user_id,
                        'product_id': user_product.product_id,
                        'product_name': user_product.product_name,
                        'order_cnt': user_product.order_cnt
                    }
                )
