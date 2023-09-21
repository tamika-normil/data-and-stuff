with gms as (
      select receipt_id
        ,sum(coalesce(t.gms_net,0)) as gms_net
        ,sum(coalesce(t.gms_gross,0)) as gms_gross
      from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t   -- change to receipts_gms
      group by 1
      ),
exchange AS (
    SELECT
        source_currency,
        source_precision,
        target_currency,
        target_precision,
        market_rate,
        seller_rate,
        buyer_rate,
        create_date,
        date,
        creation_tsz,
        lead(create_date, 1) OVER (PARTITION BY source_currency, target_currency ORDER BY create_date) - 1 AS cw_thru_date
      FROM
        `etsy-data-warehouse-prod.materialized.exchange_rates`),
  affiliate AS (
    SELECT
        DATE(transaction_date) AS day,
        concat(substr(CAST(publisher_id as STRING), 1, 80), ' - ', region) AS account_name,
        order_ref,
        sum(a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS cost,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales,
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` AS a
        LEFT OUTER JOIN exchange AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) )
        WHERE commission_status in ('pending','approved')
      GROUP BY 1, 2, 3 ),
eval as 
    (SELECT order_ref,sales as awin_sales, 
    receipt_usd_subtotal_price,
    trans_gms_gross,			
    trans_gms_net,
    r.gms_net,
    r.gms_gross,
    abs((sales-trans_gms_gross)/trans_gms_gross) as abs_diff,
    (sales-trans_gms_gross)/trans_gms_gross as diff
    FROM affiliate a
    left join etsy-data-warehouse-prod.transaction_mart.all_receipts t on a.order_ref = cast(t.receipt_id as string)
    left join etsy-data-warehouse-prod.transaction_mart.receipts_gms g on a.order_ref = cast(g.receipt_id as string)
    left join gms r on t.receipt_id = r.receipt_id
    WHERE day >= '2023-08-01'
    and receipt_live = 1
    and trans_gms_gross > 0 )
select count(distinct order_ref) all_receipts, count(distinct case when abs_diff > .02 then order_ref end) as pct_receipts_incorrect,
count(distinct case when diff >= .02 then order_ref end) as pct_receipts_incorrect_awin_sales_higher,
count(distinct case when diff <= -.02 then order_ref end) as pct_receipts_incorrect_awin_sales_lower
from eval ;
