INSERT INTO cdm.dm_courier_ledger (
	courier_id, courier_name, settlement_year, settlement_month,
	orders_count, orders_total_sum, rate_avg, order_processing_fee,
	courier_order_sum, courier_tips_sum, courier_reward_sum)
SELECT fn.courier_id,
	   fn.courier_name,
	   dt."year" AS settlement_year,
	   dt."month" AS settlement_month,
	   COUNT(fn.order_id) AS orders_count,
	   SUM(fn.sum) AS orders_total_sum,
	   AVG(fn.rate_avg) AS rate_avg,
	   SUM(fn.sum) * 0.25 AS order_processing_fee,
	   SUM(fn.courier_reward) AS courier_order_sum,
	   SUM(fn.tip_sum) AS courier_tips_sum,
	   (SUM(fn.courier_reward) + SUM(fn.tip_sum)) * 0.95 AS courier_reward_sum
FROM
	(WITH cr AS (
		SELECT dl.courier_id,
		   dc.courier_name,
		   avg(rate) AS rate_avg
		FROM dds.dm_deliveries dl
		LEFT JOIN dds.dm_couriers dc ON dl.courier_id = dc.id
		GROUP BY dl.courier_id, dc.courier_name),
	info AS (
		SELECT cr.courier_id,
		   cr.courier_name,
		   dd.order_id,
		   dd.delivery_ts_id,
		   dd.sum,
		   dd.tip_sum,
		   cr.rate_avg,
		   CASE
			  WHEN cr.rate_avg < 4 THEN 0.05
			  WHEN cr.rate_avg >= 4 AND cr.rate_avg < 4.5 THEN 0.07
			  WHEN cr.rate_avg >= 4.5 AND cr.rate_avg < 4.9 THEN 0.08
		   	  WHEN cr.rate_avg >= 4.9 THEN 0.1
		   END AS courier_fee,
		   CASE
			  WHEN cr.rate_avg < 4 THEN 100
			  WHEN cr.rate_avg >= 4 AND cr.rate_avg < 4.5 THEN 150
			  WHEN cr.rate_avg >= 4.5 AND cr.rate_avg < 4.9 THEN 175
		   	  WHEN cr.rate_avg >= 4.9 THEN 200
		   END AS min_fee
	FROM dds.dm_orders do2
	LEFT JOIN dds.dm_deliveries dd ON do2.id = dd.order_id
	LEFT JOIN cr ON dd.courier_id = cr.courier_id)
	SELECT courier_id,
		   courier_name,
		   order_id,
		   delivery_ts_id,
		   sum,
		   tip_sum,
		   rate_avg,
		   CASE
		      WHEN sum * courier_fee > min_fee THEN sum * courier_fee
		      ELSE min_fee
		   END AS courier_reward
	FROM info) AS fn
LEFT JOIN dds.dm_timestamps dt ON fn.delivery_ts_id = dt.id
WHERE fn.courier_id NOTNULL
GROUP BY fn.courier_id, fn.courier_name, dt."year", dt."month"
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
	SET courier_name = EXCLUDED.courier_name,
		orders_count = EXCLUDED.orders_count,
		orders_total_sum = EXCLUDED.orders_total_sum,
		rate_avg = EXCLUDED.rate_avg,
		order_processing_fee = EXCLUDED.order_processing_fee,
		courier_order_sum = EXCLUDED.courier_order_sum,
		courier_tips_sum = EXCLUDED.courier_tips_sum,
		courier_reward_sum = EXCLUDED.courier_reward_sum;