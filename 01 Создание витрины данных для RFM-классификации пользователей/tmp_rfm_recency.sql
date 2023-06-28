INSERT INTO analysis.tmp_rfm_recency (user_id, recency)
SELECT user_id,
       ntile(5) OVER (ORDER BY "time" NULLS FIRST) recency
FROM
  (WITH usid AS
     (SELECT DISTINCT id
      FROM analysis.users),
        clo AS
     (SELECT *
      FROM analysis.orders o
      WHERE o.status = 4 AND EXTRACT(YEAR FROM order_ts) >= 2022)
   		SELECT id user_id,
   			   max(order_ts) "time"
   FROM usid
   LEFT JOIN clo ON usid.id = clo.user_id
   GROUP BY id) tot;