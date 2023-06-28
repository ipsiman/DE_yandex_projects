INSERT INTO analysis.tmp_rfm_monetary_value (user_id, monetary_value)
SELECT user_id,
       ntile(5) OVER (ORDER BY tot_sum NULLS FIRST) monetary_value
FROM
  (WITH usid AS
     (SELECT DISTINCT id
      FROM analysis.users),
        clo AS
     (SELECT *
      FROM analysis.orders o
      WHERE o.status = 4 AND EXTRACT(YEAR FROM order_ts) >= 2022)
      	SELECT id user_id,
      		   SUM(cost) tot_sum
   FROM usid
   LEFT JOIN clo ON usid.id = clo.user_id
   GROUP BY id) tot;