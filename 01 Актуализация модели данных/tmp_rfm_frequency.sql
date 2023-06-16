INSERT INTO analysis.tmp_rfm_frequency (user_id, frequency)
SELECT user_id,
       ntile(5) OVER (ORDER BY tot_orders) frequency
FROM
  (WITH usid AS
     (SELECT DISTINCT id
      FROM analysis.users),
        clo AS
     (SELECT *
      FROM analysis.orders o
      WHERE o.status = 4 AND EXTRACT(YEAR FROM order_ts) >= 2022)
      	SELECT id user_id,
      		   count(order_id) tot_orders
   FROM usid
   LEFT JOIN clo ON usid.id = clo.user_id
   GROUP BY id) tot;