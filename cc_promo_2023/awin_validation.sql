with pubs_to_select as
    (select *
    from etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic
    where lower(tactic) = 'social creator co' or publisher_id = '946733'),
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
        DATE(a.transaction_date) AS day,
        concat(substr(CAST(a.publisher_id as STRING), 1, 80), ' - ', region) AS account_name,
        sum(a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS cost,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales,
        count(distinct order_ref) as orders,
        0 AS impressions,
        'affiliate' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` AS a
        join pubs_to_select p on cast(a.publisher_id as string)  = p.publisher_id
        LEFT OUTER JOIN exchange AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) )
        WHERE commission_status in ('pending','approved')
      GROUP BY 1, 2 )
select date_trunc(day,month) as month, sum(sales) as sales, sum(orders) as orders
from  affiliate
group by 1
order by 1 desc
