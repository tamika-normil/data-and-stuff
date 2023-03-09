CREATE TEMPORARY TABLE all_markt as 
     (WITH exchange AS (
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
        sum(a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS cost,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales,
        0 AS impressions,
        'affiliate' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` AS a
        LEFT OUTER JOIN exchange AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) )
        WHERE commission_status in ('pending','approved')
      GROUP BY 1, 2 ),     
 all_markt AS ( 
    SELECT
        affiliate.day,
        affiliate.account_name,
        null as campaign_id,
        null as campaign_name,
        0 as clicks,
        affiliate.cost,
        0 as impressions,
        affiliate.engine
      FROM
        affiliate    )
   select * from all_markt);   

CREATE TEMPORARY TABLE costs as   
    (SELECT date(day) as day,
    sum(cost) cost
    FROM all_markt
    group by 1);

CREATE TEMPORARY TABLE channel_overview as
    (select  date as day, 
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(attributed_attr_rev_adjusted) as rev,
    #sum(attributed_gms_ly) as gms_ly,
    #sum(attributed_gms_dly) as gms_dly,
    #sum(attributed_gms_dlly) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN( 'affiliates' )
    group by 1);

CREATE TEMPORARY TABLE channel_overview_pd  as
    (select  purchase_date as day, 
    sum(coalesce(attributed_gms,0)) as gms_pd, 
    sum(attributed_attr_rev) as rev_pd,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date` a
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN( 'affiliates')
    group by 1);

CREATE TEMPORARY TABLE daily_tracker as 
    (SELECT date(day) as day, 
    sum(cost) as cost, 
    sum(coalesce(attr_gms_est,0)) as gms, 
    sum(attr_rev_est) as rev,
    sum(purchase_date_attr_gms) gms_pd,
    sum(purchase_date_attr_rev) rev_pd
    #update table name here
    FROM  `etsy-data-warehouse-dev.rollups.affiliates_tracker`
    group by 1);

select c.day, 
c.gms,
c.rev,
a.gms,
a.rev
#safe_divide((a.gms-c.gms),c.gms) as gms,
#safe_divide((a.rev-c.rev),c.rev) as rev,
#safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
#safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
#safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from channel_overview c 
left join daily_tracker a using (day)
where c.day >= '2019-01-01'
order by 2,1 desc;

select c.day, 
c.gms_pd,
c.rev_pd,
a.gms_pd,
a.rev_pd
#safe_divide((a.gms-c.gms),c.gms) as gms,
#safe_divide((a.rev-c.rev),c.rev) as rev,
#safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
#safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
#safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from channel_overview_pd c 
left join daily_tracker a using (day)
where c.day >= '2019-01-01'
order by 2,1 desc;


select c.day, 
c.cost,
a.cost
#safe_divide((a.gms-c.gms),c.gms) as gms,
#safe_divide((a.rev-c.rev),c.rev) as rev,
#safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
#safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
#safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from costs c 
left join daily_tracker a using (day)
where c.day >= '2019-01-01'
order by 2,1 desc;
