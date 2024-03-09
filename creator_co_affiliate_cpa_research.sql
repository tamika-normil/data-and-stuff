create or replace table `etsy-data-warehouse-dev.rollups.awin_spend_data` as
(WITH awin_tab AS (
    SELECT
        s.id,
        s.url,
        s.advertiser_id,
        s.publisher_id,
        s.site_name,
        s.commission_status,
        s.commission_amount_amount,
        s.commission_amount_currency,
        s.sale_amount_amount,
        s.sale_amount_currency,
        s.ip_hash,
        s.customer_country,
        s.click_date,
        s.transaction_date,
        s.lapse_time,
        s.click_device,
        s.transaction_device,
        s.publisher_url,
        s.advertiser_country,
        s.order_ref,
        s.transaction_parts_commission_group_id,
        s.transaction_parts_amount,
        s.transaction_parts_commission_amount,
        s.transaction_parts_commission_group_code,
        s.transaction_parts_commission_group_name,
        s.payment_id,
        s.transaction_query_id,
        s.region,
        s.partition_date,
        click_ref,
        UNIX_SECONDS(CAST(s.transaction_date AS TIMESTAMP)) AS click_date_int
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` s
        left join (select distinct id, click_ref from `etsy-data-warehouse-prod.marketing.awin_transaction_data`) t using (id)

  ), b AS (
    SELECT
        b.source_currency,
        b.source_precision,
        b.target_currency,
        b.target_precision,
        b.market_rate,
        b.seller_rate,
        b.buyer_rate,
        b.create_date,
        b.date,
        b.creation_tsz,
        lead(b.create_date, 1) OVER (PARTITION BY b.source_currency, b.target_currency ORDER BY b.create_date) - 1 AS cw_thru_date
      FROM
        `etsy-data-warehouse-prod.materialized.exchange_rates` AS b
  )
    SELECT
        DATE(a.transaction_date) AS day,
        DATE(a.click_date) AS click_day,
        publisher_id,
        region, 
        CASE
          WHEN a.customer_country = 'NL' THEN 'NL'
          WHEN a.customer_country = 'US' THEN 'US'
          WHEN a.customer_country = 'DE' THEN 'DE'
          WHEN a.customer_country = 'ES' THEN 'ES'
          WHEN a.customer_country = 'JP' THEN 'JP'
          WHEN a.customer_country = 'CA' THEN 'CA'
          WHEN a.customer_country = 'IN' THEN 'IN'
          WHEN a.customer_country = 'GB' THEN 'GB'
          WHEN a.customer_country = 'IT' THEN 'IT'
          WHEN a.customer_country = 'AU' THEN 'AU'
          WHEN a.customer_country = 'FR' THEN 'FR'
          ELSE 'ROW'
        END AS buyer_region,
        CASE
          WHEN a.region = 'AU' THEN 'AU'
          WHEN a.region = 'CA' THEN 'CA'
          WHEN a.region = 'DE' THEN 'DE'
          WHEN a.region = 'ES' THEN 'ES'
          WHEN a.region = 'EU' THEN 'EU'
          WHEN a.region = 'FR' THEN 'FR'
          WHEN a.region = 'UK' THEN 'GB'
          WHEN a.region = 'IT' THEN 'IT'
          WHEN a.region = 'NL' THEN 'NL'
          WHEN a.region = 'SC' THEN 'SC'
          WHEN a.region = 'US' THEN 'US'
          ELSE 'ROW'
        END AS marketing_region,
        order_ref,
        case when CAST(a.publisher_id as STRING) =  '946733' then click_ref else '0' end as subnetwork_id,
        sum(case when r.receipt_id is not null and commission_status = 'pending' and DATE(a.transaction_date) >= '2023-09-12' then 
            GREATEST( (a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) * .07 , a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) else (a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) end) AS cost,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales
      FROM
        awin_tab AS a
        LEFT OUTER JOIN b AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND a.click_date_int BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, a.click_date_int)
        LEFT OUTER JOIN etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts r on order_ref = cast(receipt_id as string) and channel = 6
        WHERE commission_status in ('pending','approved')
        and DATE(a.transaction_date) >= '2023-01-01'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8);

with base as (select date_trunc(day, month) as transaction_month, 
date_diff(day, click_day, day) as date_between_click_purchase,
reporting_channel_group, 
tactic,
count(distinct order_ref) as orders,
sum(cost) as costs, 
from `etsy-data-warehouse-dev.rollups.awin_spend_data` b
left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic ap on cast(b.publisher_id as string) = ap.publisher_id
where lower(ap.reporting_channel_group) like '%social creator co%'
group by 1,2,3,4),
get_max as 
(select date_trunc(day, month) as transaction_month, 
reporting_channel_group, 
tactic,
count(distinct order_ref) as tot_orders,
sum(cost) as tot_costs, 
from `etsy-data-warehouse-dev.rollups.awin_spend_data` b
left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic ap on cast(b.publisher_id as string) = ap.publisher_id
where lower(ap.reporting_channel_group) like '%social creator co%'
group by 1,2,3)
select b.transaction_month, b.date_between_click_purchase, b.reporting_channel_group, b.tactic, 
sum(costs) over (partition by b.transaction_month, tactic order by date_between_click_purchase asc) as costs,
tot_costs
from base b
left join get_max g using (transaction_month, tactic)
