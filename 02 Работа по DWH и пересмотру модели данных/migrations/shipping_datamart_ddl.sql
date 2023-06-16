/*
drop tables:
- shipping_status
- shipping_info
- shipping_transfer
- shipping_country_rates
- shipping_agreement
*/
DROP TABLE IF EXISTS shipping_status CASCADE;
DROP TABLE IF EXISTS shipping_info CASCADE;
DROP TABLE IF EXISTS shipping_transfer CASCADE;
DROP TABLE IF EXISTS shipping_country_rates CASCADE;
DROP TABLE IF EXISTS shipping_agreement CASCADE;


-- create shipping_country_rates
CREATE TABLE shipping_country_rates (
	shipping_country_id serial NOT NULL,
	shipping_country text,
	shipping_country_base_rate numeric(14, 3),
	PRIMARY KEY (shipping_country_id),
	CONSTRAINT unique_country_country_base_rate 
		UNIQUE (shipping_country, shipping_country_base_rate)
);


-- create shipping_agreement
CREATE TABLE shipping_agreement (
	agreement_id int8,
	agreement_number text,
	agreement_rate numeric(14, 3),
	agreement_commission numeric(14, 3),
	PRIMARY KEY (agreement_id)
);


-- create shipping_transfer
CREATE TABLE shipping_transfer (
	transfer_type_id serial NOT NULL,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(14, 3),
	PRIMARY KEY (transfer_type_id)
);


-- create shipping_info
CREATE TABLE shipping_info (
	shipping_id int8 NOT NULL,
	vendor_id int8,
	payment_amount numeric(14, 2),
	shipping_plan_datetime timestamp,
	transfer_type_id int8,
	shipping_country_id int8,
	agreement_id int8,
	PRIMARY KEY (shipping_id),
	CONSTRAINT shipping_info_transfer_type_id_fkey
		FOREIGN KEY (transfer_type_id) REFERENCES shipping_transfer (transfer_type_id) ON UPDATE CASCADE,
	CONSTRAINT shipping_info_shipping_country_id_fkey
		FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates (shipping_country_id) ON UPDATE CASCADE,
	CONSTRAINT shipping_agreement_id_fkey
		FOREIGN KEY (agreement_id) REFERENCES shipping_agreement (agreement_id) ON UPDATE CASCADE
);


-- create shipping_status
CREATE TABLE shipping_status (
	shipping_id int8 NOT NULL,
	status text,
	state text,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp,
	PRIMARY KEY (shipping_id)
);
