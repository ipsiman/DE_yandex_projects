import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    date: datetime
    cost: float
    payment: float
    final_status: str
    restaurant: Dict
    user: Dict
    products: List[Dict]


# А это нормально, что мы нарушаем правила именований?
# Или в инженерии допустимы такие именования - H_User, L_OrderUser?
class HubOrder(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str


class HubUser(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class HubRestaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str


class HubProduct(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str


class HubCategory(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


class LinkOrderUser(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class LinkOrderProduct(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class LinkProductCategory(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class LinkProductRestaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class SatOrderCost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: uuid.UUID


class SatOrderStatus(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: uuid.UUID


class SatRestaurantNames(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: uuid.UUID


class SatUserNames(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: uuid.UUID


class SatProductNames(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: uuid.UUID


class OrderDdsBuilder:
    def __init__(self, order: OrderObj) -> None:
        self._order = order
        self.source_system = 'orders-system-kafka'

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=uuid.NAMESPACE_DNS, name=str(obj))

    def h_order(self) -> HubOrder:
        return HubOrder(
            h_order_pk=self._uuid(self._order.id),
            order_id=self._order.id,
            order_dt=self._order.date,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_user(self) -> HubUser:
        return HubUser(
            h_user_pk=self._uuid(self._order.user['id']),
            user_id=self._order.user['id'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_restaurant(self) -> HubRestaurant:
        return HubRestaurant(
            h_restaurant_pk=self._uuid(self._order.restaurant['id']),
            restaurant_id=self._order.restaurant['id'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[HubProduct]:
        products = []

        for it in self._order.products:
            products.append(
                HubProduct(
                    h_product_pk=self._uuid(it['id']),
                    product_id=it['id'],
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products

    def h_category(self) -> List[HubCategory]:
        categories = []

        for it in self._order.products:
            categories.append(
                HubCategory(
                    h_category_pk=self._uuid(it['category']),
                    category_name=it['category'],
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return categories

    def l_order_user(self) -> LinkOrderUser:
        order_pk = self._uuid(self._order.id)
        user_pk = self._uuid(self._order.user['id'])
        return LinkOrderUser(
            hk_order_user_pk=self._uuid(str(order_pk) + str(user_pk)),
            h_order_pk=order_pk,
            h_user_pk=user_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def l_order_product(self) -> List[LinkOrderProduct]:
        order_product_list = []

        for it in self._order.products:
            order_pk = self._uuid(self._order.id)
            product_pk = self._uuid(it['id'])
            order_product_list.append(
                LinkOrderProduct(
                    hk_order_product_pk=self._uuid(str(order_pk) + str(product_pk)),
                    h_order_pk=order_pk,
                    h_product_pk=product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return order_product_list

    def l_product_category(self) -> List[LinkProductCategory]:
        product_category_list = []

        for it in self._order.products:
            product_pk = self._uuid(it['id'])
            category_pk = self._uuid(it['category'])
            product_category_list.append(
                LinkProductCategory(
                    hk_product_category_pk=self._uuid(str(product_pk) + str(category_pk)),
                    h_product_pk=product_pk,
                    h_category_pk=category_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_category_list

    def l_product_restaurant(self) -> List[LinkProductRestaurant]:
        product_restaurant_list = []

        for it in self._order.products:
            product_pk = self._uuid(it['id'])
            restaurant_pk = self._uuid(self._order.restaurant['id'])
            product_restaurant_list.append(
                LinkProductRestaurant(
                    hk_product_restaurant_pk=self._uuid(str(product_pk) + str(restaurant_pk)),
                    h_product_pk=product_pk,
                    h_restaurant_pk=restaurant_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_restaurant_list

    def s_order_cost(self) -> SatOrderCost:
        order_pk = self._uuid(self._order.id)
        cost = self._order.cost
        payment = self._order.payment
        return SatOrderCost(
            h_order_pk=order_pk,
            cost=cost,
            payment=payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(str(order_pk) + str(cost) + str(payment))
        )

    def s_order_status(self) -> SatOrderStatus:
        order_pk = self._uuid(self._order.id)
        status = self._order.final_status
        return SatOrderStatus(
            h_order_pk=order_pk,
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(str(order_pk) + str(status))
        )

    def s_restaurant_names(self) -> SatRestaurantNames:
        restaurant_pk = self._uuid(self._order.restaurant['id'])
        name = self._order.restaurant['name']
        return SatRestaurantNames(
            h_restaurant_pk=restaurant_pk,
            name=name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(str(restaurant_pk) + str(name))
        )

    def s_user_names(self) -> SatUserNames:
        user_pk = self._uuid(self._order.user['id'])
        username = self._order.user['name']
        userlogin = self._order.user['login']
        return SatUserNames(
            h_user_pk=user_pk,
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(str(user_pk) + str(username) + str(userlogin))
        )

    def s_product_names(self) -> List[SatProductNames]:
        product_names_list = []

        for it in self._order.products:
            product_pk = self._uuid(it['id'])
            name = it['name']
            product_names_list.append(
                SatProductNames(
                    h_product_pk=product_pk,
                    name=name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(str(product_pk) + str(name))
                )
            )
        return product_names_list


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_order_insert(self, order: HubOrder) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order (
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO UPDATE SET 
                            h_order_pk=EXCLUDED.h_order_pk,
                            order_id=EXCLUDED.order_id,
                            order_dt=EXCLUDED.order_dt,
                            load_dt=EXCLUDED.load_dt,
                            load_src=EXCLUDED.load_src;
                    """,
                    {
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_dt': order.load_dt,
                        'load_src': order.load_src
                    }
                )

    def h_user_insert(self, user: HubUser) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user (
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO UPDATE SET 
                            h_user_pk=EXCLUDED.h_user_pk,
                            user_id=EXCLUDED.user_id,
                            load_dt=EXCLUDED.load_dt,
                            load_src=EXCLUDED.load_src;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def h_restaurant_insert(self, restaurant: HubRestaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO UPDATE SET 
                            h_restaurant_pk=EXCLUDED.h_restaurant_pk,
                            restaurant_id=EXCLUDED.restaurant_id,
                            load_dt=EXCLUDED.load_dt,
                            load_src=EXCLUDED.load_src;
                    """,
                    {
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_dt': restaurant.load_dt,
                        'load_src': restaurant.load_src
                    }
                )

    def h_product_insert(self, product: HubProduct) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO UPDATE SET 
                            h_product_pk=EXCLUDED.h_product_pk,
                            product_id=EXCLUDED.product_id,
                            load_dt=EXCLUDED.load_dt,
                            load_src=EXCLUDED.load_src;
                    """,
                    {
                        'h_product_pk': product.h_product_pk,
                        'product_id': product.product_id,
                        'load_dt': product.load_dt,
                        'load_src': product.load_src
                    }
                )

    def h_category_insert(self, category: HubCategory) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO UPDATE SET 
                            h_category_pk=EXCLUDED.h_category_pk,
                            category_name=EXCLUDED.category_name,
                            load_dt=EXCLUDED.load_dt,
                            load_src=EXCLUDED.load_src;
                    """,
                    {
                        'h_category_pk': category.h_category_pk,
                        'category_name': category.category_name,
                        'load_dt': category.load_dt,
                        'load_src': category.load_src
                    }
                )

    def l_order_user_insert(self, order_user: LinkOrderUser) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user (
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': order_user.hk_order_user_pk,
                        'h_order_pk': order_user.h_order_pk,
                        'h_user_pk': order_user.h_user_pk,
                        'load_dt': order_user.load_dt,
                        'load_src': order_user.load_src
                    }
                )

    def l_order_product_insert(self, order_product: LinkOrderProduct) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product (
                            hk_order_product_pk,
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': order_product.hk_order_product_pk,
                        'h_order_pk': order_product.h_order_pk,
                        'h_product_pk': order_product.h_product_pk,
                        'load_dt': order_product.load_dt,
                        'load_src': order_product.load_src
                    }
                )

    def l_product_category_insert(self, product_category: LinkProductCategory) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category (
                            hk_product_category_pk,
                            h_product_pk,
                            h_category_pk,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(hk_product_category_pk)s,
                            %(h_product_pk)s,
                            %(h_category_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': product_category.hk_product_category_pk,
                        'h_product_pk': product_category.h_product_pk,
                        'h_category_pk': product_category.h_category_pk,
                        'load_dt': product_category.load_dt,
                        'load_src': product_category.load_src
                    }
                )

    def l_product_restaurant_insert(self, product_restaurant: LinkProductRestaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant (
                            hk_product_restaurant_pk,
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                        )
                        VALUES (
                            %(hk_product_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(h_restaurant_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': product_restaurant.hk_product_restaurant_pk,
                        'h_product_pk': product_restaurant.h_product_pk,
                        'h_restaurant_pk': product_restaurant.h_restaurant_pk,
                        'load_dt': product_restaurant.load_dt,
                        'load_src': product_restaurant.load_src
                    }
                )

    def s_order_cost_insert(self, order_cost: SatOrderCost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost (
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff)
                        VALUES (
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'load_dt': order_cost.load_dt,
                        'load_src': order_cost.load_src,
                        'hk_order_cost_hashdiff': order_cost.hk_order_cost_hashdiff,
                    }
                )

    def s_order_status_insert(self, order_status: SatOrderStatus) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status (
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES (
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'load_dt': order_status.load_dt,
                        'load_src': order_status.load_src,
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )

    def s_restaurant_names_insert(self, restaurant_names: SatRestaurantNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names (
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES (
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant_names.h_restaurant_pk,
                        'name': restaurant_names.name,
                        'load_dt': restaurant_names.load_dt,
                        'load_src': restaurant_names.load_src,
                        'hk_restaurant_names_hashdiff': restaurant_names.hk_restaurant_names_hashdiff
                    }
                )

    def s_user_names_insert(self, user_names: SatUserNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names (
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES (
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (h_user_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user_names.h_user_pk,
                        'username': user_names.username,
                        'userlogin': user_names.userlogin,
                        'load_dt': user_names.load_dt,
                        'load_src': user_names.load_src,
                        'hk_user_names_hashdiff': user_names.hk_user_names_hashdiff
                    }
                )

    def s_product_names_insert(self, product_names: SatProductNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names (
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES (
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_product_pk': product_names.h_product_pk,
                        'name': product_names.name,
                        'load_dt': product_names.load_dt,
                        'load_src': product_names.load_src,
                        'hk_product_names_hashdiff': product_names.hk_product_names_hashdiff
                    }
                )
