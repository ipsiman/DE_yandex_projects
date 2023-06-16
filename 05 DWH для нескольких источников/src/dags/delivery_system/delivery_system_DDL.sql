-- DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id serial PRIMARY KEY NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year smallint NOT NULL,
	settlement_month smallint NOT NULL,
	orders_count int NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	rate_avg numeric(3, 2) NOT NULL CHECK (rate_avg > 0),
	order_processing_fee numeric(14, 2) NOT NULL CHECK (rate_avg > 0),
	courier_order_sum numeric(14, 2) NOT NULL CHECK (rate_avg > 0),
	courier_tips_sum numeric(14, 2) NOT NULL CHECK (rate_avg > 0),
	courier_reward_sum numeric(14, 2) NOT NULL CHECK (rate_avg > 0),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_courier_id_year_month_uindex UNIQUE (courier_id, settlement_year, settlement_month)
);

-- dm_couriers - нету, необходимо создать
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id serial not null primary key,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_courier_id_uindex UNIQUE (courier_id)
);

-- dm_deliveries - нету, необходимо создать
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id serial NOT NULL PRIMARY KEY,
	delivery_id varchar NOT NULL,
	delivery_ts_id int NOT NULL,
	order_id int NOT NULL,
	courier_id int NOT NULL,
	rate smallint check ((rate >= 0) and (rate <= 5)),
	"sum" numeric(14, 2) not null DEFAULT 0,
	tip_sum numeric(14, 2) not null DEFAULT 0,
CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
CONSTRAINT dm_deliveries_delivery_ts_id_fkey FOREIGN KEY (delivery_ts_id) REFERENCES dds.dm_timestamps(id)
);

-- dm_orders - есть, необходимо доработать, добавить поле courier_id

-- DROP TABLE IF EXISTS dds.dm_orders CASCADE;
CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	courier_id int4 NULL,
	CONSTRAINT dm_orders_order_key_uindex UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
);

ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);
ALTER table dds.dm_orders ADD CONSTRAINT dm_orders_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);


-- DROP TABLE IF EXISTS stg.deliverysystem_couriers;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
	id serial4 NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT deliverysystem_couriers_object_value_key UNIQUE (object_value),
	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id)
);


-- DROP TABLE IF EXISTS stg.deliverysystem_deliveries;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
	id serial4 NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT deliverysystem_deliveries_object_value_key UNIQUE (object_value),
	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id)
);
