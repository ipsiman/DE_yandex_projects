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





TRUNCATE TABLE USERYANDEXRU__STAGING.currencies;

SELECT *
FROM USERYANDEXRU__STAGING.transactions
WHERE transaction_dt::date = '2022-10-01';

DELETE FROM USERYANDEXRU__STAGING.transactions
WHERE transaction_dt::date = '2022-10-01';


INSERT INTO USERYANDEXRU__DWH.global_metrics

(WITH inf AS
  (SELECT CURRENT_DATE()  AS date_update,
          t.operation_id,
          t.account_number_from,
          t.country,
          t.currency_code,
          c.currency_with_div,
          ROUND(t.amount * c.currency_with_div) / 100 AS amount_us
   FROM USERYANDEXRU__STAGING.transactions t
   LEFT JOIN USERYANDEXRU__STAGING.currencies c ON t.transaction_dt::date = c.date_update::date
     AND t.currency_code = c.currency_code
   WHERE t.transaction_dt::date = '2022-10-18'
     AND c.currency_code_with = 420
     AND t.status = 'done'
     AND t.account_number_from > 0
     AND t.account_number_to > 0
     AND t.transaction_type in ('c2a_incoming',
                                'c2b_partner_incoming',
                                'sbp_incoming',
                                'sbp_outgoing',
                                'transfer_incoming',
                                'transfer_outgoing'))
SELECT date_update,
       currency_code AS currency_from,
       country,
       SUM(amount_us) AS amount_total,
       COUNT(operation_id) AS cnt_transactions,
       COUNT(operation_id) / COUNT(DISTINCT account_number_from) AS avg_transactions_per_account,
       COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
FROM inf
GROUP BY 1, 2, 3);


TRUNCATE TABLE USERYANDEXRU__DWH.global_metrics

SELECT * FROM USERYANDEXRU__DWH.global_metrics
ORDER BY 1


SELECT COUNT(*) 
FROM USERYANDEXRU__STAGING.transactions
ORDER BY 1

SELECT DISTINCT transaction_dt::date
   FROM USERYANDEXRU__STAGING.transactions t
   LEFT JOIN USERYANDEXRU__STAGING.currencies c ON t.transaction_dt::date = c.date_update::date
     AND t.currency_code = c.currency_code
   WHERE t.status = 'done'







