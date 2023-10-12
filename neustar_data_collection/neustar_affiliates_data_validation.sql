-- latest data validation https://docs.google.com/spreadsheets/d/1Ll06vfLMyOMt-gU3mJU6jb7z0ugE8juhJU1UYs6GTgA/edit#gid=1779331903
create temp table awin_costs as 
with exchange AS (
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
        `etsy-data-warehouse-prod.materialized.exchange_rates`)
    SELECT
        DATE(transaction_date) AS day,
        concat(substr(CAST(publisher_id as STRING), 1, 80), ' - ', region) AS account_name,
        case when region = 'UK' then 'GB' else region end as country,    
        sum(a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS cost,
        sum(a.commission_amount_amount) as cost_no_exchange,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales,
        0 AS impressions,
        'affiliate' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` AS a
        LEFT OUTER JOIN exchange AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) )
         where commission_status in ('pending','approved')
         AND region in ('US','DE','CA','FR','GB', 'UK')
      GROUP BY 1, 2, 3;

/*
create temp table performance_marketing_daily_tracker as
    (select *,  
    case when reporting_channel_group = 'Affiliate' then split(account_name,' ')[SAFE_OFFSET(0)] else account_name end as publisher_id,
    case when lower(account_name) like '% us%' then 'US'
            when lower(account_name) like '% uk%' then 'GB'
            when lower(account_name) like '% ca%' then 'CA'
            when lower(account_name) like '% fr%' then 'FR'
            when lower(account_name) like '% au%' then 'AU'
            when lower(account_name) like '% de%' then 'DE'
            when lower(account_name) like '% ie%' then 'IE'
            when lower(account_name) like '% it%' then 'IT'
            when lower(account_name) like '% nl%' then 'NL'
            when lower(account_name) like '% at%' then 'AT'
            when lower(account_name) like '% be%' then 'BE'
            when lower(account_name) like '% ch%' then 'CH'
            when lower(account_name) like '% es%' then 'ES'
            when lower(account_name) like '% no %' then 'NO'
            when lower(account_name) like '% fi %' then 'FI'
            when lower(account_name) like '% se %' then 'SE'
            when lower(account_name) like '% dk %' then 'DK'
            when lower(account_name) like '% mx %' then 'MX'
            when lower(account_name) like '% nz %' then 'NZ'
            when lower(account_name) like '% in %' then 'IN'
            when lower(account_name) like '%facebook%' and lower(account_name) like 'facebook -%' then 'US'
            when lower(account_name) = 'Facebook Video - Thruplay' then 'US'
            else 'Other Country'
            end as country
    from etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker);
*/

with neustar as 
(SELECT date, sum(cost) as cost, sum(visits) as visits 
FROM `etsy-data-warehouse-dev.rollups.neustar_etl_affiliates` 
#performance_marketing_daily_tracker
where date >= date_sub(current_date(), interval 3 quarter)
#and country in ('US','DE','CA','FR','GB')
#and reporting_channel_group = 'Affiliate'
group by 1),
channel_overview as 
(SELECT date, sum(visits) as visits 
FROM `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` 
where channel_group = 'Affiliates'
and key_market in ('US','DE','CA','FR','GB')
group by 1),
awin_costs_agg as 
(SELECT date(day) as date, sum(cost) as cost
FROM  awin_costs 
where day >= date_sub(current_date(), interval 3 quarter)
group by 1)
select neustar.*, co.*, a.*
from neustar
left join channel_overview co using (date)
left join  awin_costs_agg a using (date);

-- market level check 
with neustar as 
(SELECT date, country, sum(cost) as cost, sum(visits) as visits, sum(impressions) as impressions 
FROM `etsy-data-warehouse-dev.rollups.neustar_etl_affiliates` 
#performance_marketing_daily_tracker
where date >= date_sub(current_date(), interval 3 quarter)
#and country in ('US','DE','CA','FR','GB')
#and reporting_channel_group = 'Affiliate'
group by 1,2),
channel_overview as 
(SELECT date, key_market as country, sum(visits) as visits 
FROM `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` 
where channel_group = 'Affiliates'
and key_market in ('US','DE','CA','FR','GB')
group by 1,2),
awin_costs_agg as 
(SELECT date(day) as date, country, sum(cost) as cost
FROM  awin_costs 
where day >= date_sub(current_date(), interval 3 quarter)
group by 1,2)
select neustar.*, co.*, a.*
from neustar
left join channel_overview co using (date, country)
left join  awin_costs_agg a using (date, country);

-- check impressions data

with base as (SELECT date(post_date) as post_date, 
  case when campaign_name like '%(USA)%' then 'US'
  when campaign_name like '%(CA)%' then 'CA'
  when campaign_name like '%(AU)%' then 'AU'
  when campaign_name like '%(UK)%' then 'GB'
  when campaign_name like '%(FR)%' then 'FR'
  when campaign_name like '%(DE)%' then 'DE'
  when campaign_name like '%(Rest of World)%' then 'Row'
  when campaign_name in ('Tanya Burr Creator Drop') then 'GB'
  else 'US' end as country,
  sum(Impressions) as Impressions
  FROM etsy-data-warehouse-prod.static.historical_ciq_engagement
  group by 1,2)
  select post_date, sum(impressions) as imp from base where post_date >= '2023-01-12' and country in ('US','DE','CA','FR','GB') group by 1 order by 1 asc;
