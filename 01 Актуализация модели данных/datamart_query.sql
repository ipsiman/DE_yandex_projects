INSERT INTO analysis.dm_rfm_segments (user_id, recency, frequency, monetary_value)
WITH usid AS
  (SELECT DISTINCT id
   FROM production.users)
SELECT usid.id user_id,
       trr.recency,
       trf.frequency,
       trmv.monetary_value
FROM usid
LEFT JOIN analysis.tmp_rfm_recency trr ON usid.id = trr.user_id
LEFT JOIN analysis.tmp_rfm_frequency trf ON usid.id = trf.user_id
LEFT JOIN analysis.tmp_rfm_monetary_value trmv ON usid.id = trmv.user_id
ORDER BY usid.id;
