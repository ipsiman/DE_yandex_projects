#### Состав витрины:
- `id` — идентификатор записи.
- `courier_id` — ID курьера, которому перечисляем.
- `courier_name` — Ф. И. О. курьера.
- `settlement_year` — год отчёта.
- `settlement_month` — месяц отчёта, где 1 — январь и 12 — декабрь.
- `orders_count` — количество заказов за период (месяц).
- `orders_total_sum` — общая стоимость заказов.
- `rate_avg` — средний рейтинг курьера по оценкам пользователей.
- `order_processing_fee` — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
- `courier_order_sum` — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
- `courier_tips_sum` — сумма, которую пользователи оставили курьеру в качестве чаевых.
- `courier_reward_sum` — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

### 1. Список полей, которые необходимы для витрины.
- courier_id
- courier_name
- order_id
- delivery_ts
- rate
- sum
- tip_sum

### 2. Необходимые таблицы в слое DDS из которых возьмём поля для витрины.
- dm_orders - есть, необходимо доработать, добавить поле courier_id
- dm_timestamps
- dm_couriers - нету, необходимо создать
- dm_deliveries - нету, необходимо создать

### 3. На основе списка таблиц в DDS составим список сущностей и полей, которые необходимо загрузить из API.
Из API необходимо загрузить информацию из источников `/couriers` и `/deliveries`. Информацию из источника в слой STG будем выгружать "как есть" в формате json.


В таблицы слоя DDS будем переносить поля:
- `courier_id` и `courier_name` из API источника `/couriers`.
- 'order_id' и 'delivery_ts' из API источника `/deliveries` расчет с курьерами делаем по дате открытия заказа.
- `rate`, `sum` и `tip_sum` из API источника `/deliveries` для расчета финансовой отчетности.
