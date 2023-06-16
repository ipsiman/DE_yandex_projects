DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
	id serial,
	new_customers_count int,
	returning_customers_count int,
	refunded_customer_count int,
	period_name varchar(10) DEFAULT 'weekly',
	period_id int,
	item_id int NOT NULL,
	new_customers_revenue numeric(10, 2),
	returning_customers_revenue numeric(10, 2),
	customers_refunded int,
	PRIMARY KEY (id),
	FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);