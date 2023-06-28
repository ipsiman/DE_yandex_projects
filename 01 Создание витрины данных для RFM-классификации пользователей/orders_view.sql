CREATE VIEW analysis.orders AS
SELECT o.order_id,
       o.order_ts,
       o.user_id,
       o.bonus_payment,
       o.payment,
       o."cost",
       o.bonus_grant,
       osl.status_id status
FROM production.orders o
LEFT JOIN
  (SELECT DISTINCT ON (o.order_id) o.order_id,
                       o.status_id,
                       o.dttm
   FROM production.orderstatuslog o
   ORDER BY o.order_id,
            o.dttm DESC) osl ON o.order_id = osl.order_id;