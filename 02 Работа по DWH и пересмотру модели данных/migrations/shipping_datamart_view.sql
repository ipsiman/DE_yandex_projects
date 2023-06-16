-- drop view shipping_datamart
DROP VIEW IF EXISTS shipping_datamart;


-- create view shipping_datamart
CREATE VIEW shipping_datamart AS
SELECT sh.shipping_id,
       si.vendor_id,
       st.transfer_type,
       date_part('day', ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) full_day_at_shipping,
       CASE
           WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1
           ELSE 0
       END is_delay,
       CASE
           WHEN ss.status = 'finished' THEN 1
           ELSE 0
       END is_shipping_finish,
       CASE
           WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN date_part('day', ss.shipping_end_fact_datetime - shipping_plan_datetime)
           ELSE 0
       END delay_day_at_shipping,
       si.payment_amount,
       si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) vat,
       si.payment_amount * sa.agreement_commission profit
FROM
  (SELECT DISTINCT shippingid shipping_id
   FROM shipping) sh
LEFT JOIN shipping_info si ON sh.shipping_id = si.shipping_id
LEFT JOIN shipping_transfer st ON si.transfer_type_id = st.transfer_type_id
LEFT JOIN shipping_status ss ON sh.shipping_id = ss.shipping_id
LEFT JOIN shipping_country_rates scr ON si.shipping_country_id = scr.shipping_country_id
LEFT JOIN shipping_agreement sa ON si.agreement_id = sa.agreement_id;
