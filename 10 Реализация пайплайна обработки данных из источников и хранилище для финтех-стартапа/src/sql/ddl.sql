--DROP TABLE IF EXISTS USERYANDEXRU__STAGING.transactions;
CREATE TABLE IF NOT EXISTS USERYANDEXRU__STAGING.transactions
(
    operation_id uuid NOT NULL,
    account_number_from int NOT NULL,
    account_number_to int NOT NULL,
    currency_code numeric(3) NOT NULL,
    country varchar(100) NOT NULL,
    status varchar(100) NOT NULL,
    transaction_type varchar(100) NOT NULL,
    amount int NOT NULL,
    transaction_dt timestamp(3) NOT NULL
)
PARTITION BY ((transactions.transaction_dt)::date);

CREATE PROJECTION IF NOT EXISTS USERYANDEXRU__STAGING.transactions (
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
) AS
    SELECT transactions.operation_id,
           transactions.account_number_from,
           transactions.account_number_to,
           transactions.currency_code,
           transactions.country,
           transactions.status,
           transactions.transaction_type,
           transactions.amount,
           transactions.transaction_dt
    FROM USERYANDEXRU__STAGING.transactions
    ORDER BY transactions.transaction_dt, transactions.operation_id
SEGMENTED BY hash(transactions.transaction_dt, transactions.operation_id) ALL NODES KSAFE 1;


--DROP TABLE IF EXISTS USERYANDEXRU__STAGING.currencies;
CREATE TABLE IF NOT EXISTS USERYANDEXRU__STAGING.currencies
(
    date_update timestamp(3) NOT NULL,
    currency_code numeric(3) NOT NULL,
    currency_code_with numeric(3) NOT NULL,
    currency_with_div numeric(5,2) NOT NULL
)
PARTITION BY ((currencies.date_update)::date);

CREATE PROJECTION IF NOT EXISTS USERYANDEXRU__STAGING.currencies (
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
) AS
    SELECT currencies.date_update,
           currencies.currency_code,
           currencies.currency_code_with,
           currencies.currency_with_div
    FROM USERYANDEXRU__STAGING.currencies
    ORDER BY currencies.date_update, currencies.currency_code
SEGMENTED BY hash(currencies.date_update, currencies.currency_code) ALL NODES KSAFE 1;


--DROP TABLE IF EXISTS USERYANDEXRU__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS USERYANDEXRU__DWH.global_metrics
(
    date_update timestamp(3) NOT NULL,
    currency_from numeric(3) NOT NULL,
    country varchar(100) NOT NULL,
    amount_total numeric(15,2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(15,2) NOT NULL,
    cnt_accounts_make_transactions int NOT NULL
)
PARTITION BY ((global_metrics.date_update)::date);
