INSERT INTO USERYANDEXRU__DWH.global_metrics (
    date_update,
    currency_from,
    country,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
(WITH inf AS
  (SELECT (%s)::timestamp AS date_update, --update_date
          t.operation_id,
          t.account_number_from,
          t.country,
          t.currency_code,
          c.currency_with_div,
          ROUND(t.amount * c.currency_with_div) / 100 AS amount_us
   FROM USERYANDEXRU__STAGING.transactions t
   LEFT JOIN USERYANDEXRU__STAGING.currencies c ON t.transaction_dt::date = c.date_update::date
     AND t.currency_code = c.currency_code
   WHERE t.transaction_dt::date = (%s) --load_date
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
GROUP BY 1, 2, 3)
