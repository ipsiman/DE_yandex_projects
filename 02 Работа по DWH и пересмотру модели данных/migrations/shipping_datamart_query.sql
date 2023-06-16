-- insert data to shipping_country_rates
INSERT INTO shipping_country_rates(shipping_country, shipping_country_base_rate)
SELECT DISTINCT
	shipping_country,
	shipping_country_base_rate
FROM shipping;


-- insert data to shipping_agreement
INSERT INTO shipping_agreement (
	agreement_id,
	agreement_number,
	agreement_rate,
	agreement_commission
)
SELECT
	description[1]::int8 agreement_id,
	description[2] agreement_number,
	description[3]::numeric(14, 3) agreement_rate,
	description[4]::numeric(14, 3) agreement_commission
FROM
	(SELECT DISTINCT regexp_split_to_array(vendor_agreement_description, ':+') description
	FROM shipping) AS vad;


-- insert data to shipping_transfer
INSERT INTO shipping_transfer (
	transfer_type,
	transfer_model,
	shipping_transfer_rate
)
SELECT
	description[1],
	description[2],
	shipping_transfer_rate
FROM
	(SELECT 
		DISTINCT regexp_split_to_array(shipping_transfer_description, ':+') description,
		shipping_transfer_rate
	FROM shipping) std;


-- insert data to shipping_info
INSERT INTO shipping_info (
	shipping_id,
	vendor_id,
	payment_amount,
	shipping_plan_datetime,
	transfer_type_id,
	shipping_country_id,
	agreement_id
)
SELECT 
	DISTINCT s.shippingid, 
	s.vendorid,
	s.payment_amount,
	s.shipping_plan_datetime,
	st.transfer_type_id,
	scr.shipping_country_id,
	(regexp_split_to_array(s.vendor_agreement_description, ':+'))[1]::int8 agreement_id
FROM shipping s
LEFT JOIN shipping_country_rates scr ON s.shipping_country = scr.shipping_country
LEFT JOIN shipping_transfer st
	ON (regexp_split_to_array(s.shipping_transfer_description, ':+'))[1] = st.transfer_type
		AND (regexp_split_to_array(s.shipping_transfer_description, ':+'))[2] = st.transfer_model;
		

-- insert data to shipping_status
INSERT INTO shipping_status (
	shipping_id,
	status,
	state,
	shipping_start_fact_datetime,
	shipping_end_fact_datetime
)
WITH ship_end_status AS (
	SELECT
		shippingid,
		status,
		state,
		state_datetime,
		row_number() OVER (PARTITION BY shippingid ORDER BY state_datetime DESC) AS date_rank
	FROM shipping
	), ship_date AS (
	SELECT 
	shippingid,
	min(case when state = 'booked' then state_datetime end) as shipping_start_fact_datetime,
	max(case when state = 'recieved' then state_datetime end) as shipping_end_fact_datetime
	FROM shipping
	group by shippingid
	)
SELECT
	ses.shippingid shipping_id,
	ses.status,
	ses.state,
	sd.shipping_start_fact_datetime,
	sd.shipping_end_fact_datetime
FROM ship_end_status ses
left join ship_date sd ON ses.shippingid = sd.shippingid
WHERE ses.date_rank = 1;
