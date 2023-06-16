# Витрина RFM

## 2. Качество данных

Качество данных хорошее.
Для обеспечения качества данных в таблицах используются инструменты:
- первичные и внешние ключи
- ограничения (constraints)
- правильные типы данных
Такие как:
- у всех данных стоят ограничения NOT NULL, чтобы не было пропусков
- ограничения в таблицах orderitems (UNIQUE (order_id, product_id)) и orderstatuslog (UNIQUE (order_id, status_id)) на уникальность, чтобы не было дублей
- в таблице orders проверка на правильную стоимость CHECK ((cost = (payment + bonus_payment)))
- в таблице orderitems есть проверки:
	CHECK (((discount >= (0)::numeric) AND (discount <= price))),
	CHECK ((price >= (0)::numeric)),
	CHECK ((quantity > 0))