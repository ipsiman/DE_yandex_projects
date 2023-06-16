-- удалим данные за текущий период
-- DELETE FROM mart.f_customer_retention WHERE period_id = DATE_PART('week', '{{ ds }}'::DATE);

-- вставим данные за текущий период
INSERT INTO mart.f_customer_retention (new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
(WITH customers AS
  (SELECT uol.customer_id,
          uol.status
   FROM staging.user_order_log uol
   LEFT JOIN mart.d_calendar AS dc ON uol.date_time::Date = dc.date_actual
   WHERE uol.date_time::Date = '{{ ds }}'),
new_customers AS
  (SELECT customer_id
   FROM customers
   WHERE status = 'shipped'
   GROUP BY customer_id
   HAVING count(*) = 1),
returning_customers AS
  (SELECT customer_id
   FROM customers
   WHERE status = 'shipped'
   GROUP BY customer_id
   HAVING count(*) > 1),
refunded_customers AS
  (SELECT customer_id
   FROM customers
   WHERE status = 'refunded'
   GROUP BY customer_id),
newc AS
	(SELECT count(customer_id) new_customers_count,
			item_id,
			dc.week_of_year,
			sum(quantity*payment_amount) new_customers_revenue 
	FROM staging.user_order_log uol
	LEFT JOIN mart.d_calendar AS dc ON uol.date_time::Date = dc.date_actual
	WHERE uol.date_time::Date = '{{ ds }}'
		AND customer_id in (select customer_id from new_customers)
	group by dc.week_of_year, item_id),
retc AS
	(SELECT count(customer_id) returning_customers_count,
			item_id,
			dc.week_of_year,
			sum(quantity*payment_amount) returning_customers_revenue
	FROM staging.user_order_log uol
	LEFT JOIN mart.d_calendar AS dc ON uol.date_time::Date = dc.date_actual
	WHERE uol.date_time::Date = '{{ ds }}'
		AND customer_id in (select customer_id from returning_customers)
	group by dc.week_of_year, item_id),
refc AS
	(SELECT count(customer_id) refunded_customers_count,
			item_id,
			dc.week_of_year,
			sum(quantity*payment_amount) customers_refunded
	FROM staging.user_order_log uol
	LEFT JOIN mart.d_calendar AS dc ON uol.date_time::Date = dc.date_actual
	WHERE uol.date_time::Date = '{{ ds }}'
		AND customer_id in (select customer_id from refunded_customers)
	group by dc.week_of_year, item_id)
select newc.new_customers_count,
	   retc.returning_customers_count,
	   refc.refunded_customers_count,
	   'weekly' period_name,
	   newc.week_of_year period_id,
	   newc.item_id,
	   newc.new_customers_revenue,
	   retc.returning_customers_revenue,
	   refc.customers_refunded
from newc
full join retc on newc.week_of_year = retc.week_of_year and newc.item_id = retc.item_id
full join refc on newc.week_of_year = refc.week_of_year and newc.item_id = refc.item_id)
ON CONFLICT DO NOTHING;
