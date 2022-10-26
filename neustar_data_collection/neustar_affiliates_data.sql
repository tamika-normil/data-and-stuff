-- Adele Kikuchi 
-- Neustar MMM Data Collection
-- This code snippet pulls affiliai=te-related activity on a daily level for our marketing databases
-- latest data validation

---------------------------------
-- AFFILIATE MARKETING DATA --
---------------------------------
BEGIN 

create temp table affiliate_tactics as 
  ( select distinct a.utm_content as publisher_id,
    case when t.tactic in ("Cashback", "Loyalty", "Loyalty Charity", "Coupon") then "Cashback/Loyalty/Coupon" 
    when b.publisher_id is not null or c.publisher_id is not null then "Social" 
    when t.tactic is null and b.publisher_id is null and c.publisher_id is null then 'NA'
    else t.tactic end as tactic
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` t on a.utm_content = t.publisher_id
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic_tiktok` b on a.utm_content = b.publisher_id
    left join (select distinct publisher_id from `etsy-data-warehouse-prod.rollups.influencer_cc_overview`) c on a.utm_content = c.publisher_id
    where channel_group = 'Affiliates'
    union distinct
    select distinct  cast(a.publisher_id as string)  as publisher_id,
    case when t.tactic in ("Cashback", "Loyalty", "Loyalty Charity", "Coupon") then "Cashback/Loyalty/Coupon" 
    when b.publisher_id is not null or c.publisher_id is not null then "Social" 
    when t.tactic is null and b.publisher_id is null and c.publisher_id is null then 'NA'
    else t.tactic end as tactic
    from `etsy-data-warehouse-prod.marketing.awin_spend_data` a
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` t on cast(a.publisher_id as string) = t.publisher_id
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic_tiktok` b on cast(a.publisher_id as string) = b.publisher_id
    left join (select distinct publisher_id from `etsy-data-warehouse-prod.rollups.influencer_cc_overview`) c on cast(a.publisher_id as string) = c.publisher_id);

create or replace table `etsy-data-warehouse-dev.tnormil.aff_visits` as (
with base_visits as (
  select 
  date
  ,key_market as country
  ,utm_content as publisher_id
  ,sum(visits) as visits
  from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
  where channel_group = 'Affiliates'
  and date >= '2016-01-01'
  group by 1,2,3
  )
  select 
  v.date
  ,v.country
  ,t.tactic
  ,sum(v.visits) as visits
  from base_visits v
  left join  affiliate_tactics t using (publisher_id)
  group by 1,2,3
); 

--select sum(visits) from aff_visits where date >= '2021-01-01' and country in ('US','DE','CA','FR','GB') ;
-- 37624784

create or replace table `etsy-data-warehouse-dev.tnormil.awin` as (
  with awin_base as (
    select a.*
      ,t.tactic
      from `etsy-data-warehouse-prod.marketing.awin_spend_data` a
      left join affiliate_tactics t on cast(a.publisher_id as STRING) = t.publisher_id
    where click_date >='2019-01-01'
    and commission_status in ('pending','approved')),
all_grouped_up as (
    select 
    date(transaction_date) as date
    ,customer_country as country
    ,tactic
    ,commission_amount_currency
    ,sum(commission_amount_amount) as cost
    ,sum(case when date(transaction_date) < '2022-04-01' then commission_amount_amount * 1.055
     when date(transaction_date) >= '2022-04-01' then commission_amount_amount * 1.0352 end) AS cost_with_override
    ,0 as clicks
    ,0 as visits
    from awin_base
    where customer_country in ('US','DE','CA','FR','GB')
    group by 1,2,3,4
 union all 
    select 
    date
    ,country
    ,'NA' as tactic
    ,case when country = 'US' then 'USD'
          when country = 'GB' then 'GBP' 
          when country = 'FR' then 'EUR' 
          when country = 'DE' then 'EUR' 
          when country = 'FR' then 'CAD' 
          end as commission_amount_currency
    ,sum(cost) as cost
    ,sum(case when date < '2022-04-01' then a.cost * 1.055
     when date >= '2022-04-01' then a.cost * 1.0352 end) AS cost_with_override
    ,0 as clicks
    ,0 as visits
    from `etsy-data-warehouse-dev.tnormil.awin_backfill_for_tv`
    where country in ('US','DE','CA','FR','GB')
    group by 1,2,3,4  
)
select 
date
,country
,tactic
,commission_amount_currency
,sum(cost) as cost
,sum(cost_with_override) as cost_with_override
,sum(clicks) as clicks
,sum(visits) as visits
from all_grouped_up
where date >= '2016-01-01'
group by 1,2,3,4
);


create temporary table currency_base as (
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
  coalesce(lead(b.create_date, 1) OVER (PARTITION BY b.source_currency, b.target_currency ORDER BY b.create_date) - 1, b.create_date) AS cw_thru_date
  FROM
    `etsy-data-warehouse-prod.materialized.exchange_rates` AS b
   where date >= '2016-01-01'
   and target_currency = 'USD'
);

create or replace table `etsy-data-warehouse-dev.tnormil.awin_total` as (
with awin_base as (
  select
  a.date
  ,a.country
  ,a.tactic
  ,sum(a.cost * coalesce(b_0.market_rate / 10000000, 1)) AS cost
  ,sum(a.cost_with_override * coalesce(b_0.market_rate / 10000000, 1)) AS cost_with_override
  ,sum(clicks) AS clicks
  ,sum(visits) as visits
  from
    `etsy-data-warehouse-dev.tnormil.awin` a
    left outer join currency_base as b_0
      on a.commission_amount_currency = b_0.source_currency
      and unix_seconds(cast(a.date as timestamp)) between b_0.create_date and b_0.cw_thru_date
  GROUP BY 1, 2, 3
 union all 
  select 
  date
  ,country
  ,tactic
  ,0 as cost
  ,0 as cost_with_override
  ,0 as clicks
  ,visits 
  from `etsy-data-warehouse-dev.tnormil.aff_visits`
  where country in ('US','DE','CA','FR','GB')
)
select 
date
,country
,tactic
,sum(cost) as cost
,sum(cost_with_override) as cost_with_override
,sum(clicks) as clicks
,sum(visits) as visits
from awin_base a
group by 1,2,3);


-- select 'awin_api' as source, sum(cost), sum(cost_with_override), sum(clicks), sum(visits) from awin where date >='2021-01-01' group by 1;

select * from `etsy-data-warehouse-dev.tnormil.awin_total` where date >= '2016-01-01'
;

with base as (
select 
date_trunc(date,week(monday)) as week_beginning
,date_add(date_trunc(date,week(monday)) , interval 6 day) as week_ending
,tactic
,country
,sum(cost) as cost
,sum(cost_with_override) as cost_with_override
,sum(clicks) as clicks
,sum(visits) as visits
from `etsy-data-warehouse-dev.tnormil.awin_total`  
where date >= date_sub(current_date(), interval 3 quarter)
group by 1,2,3,4
) 
select 
extract(quarter from week_beginning) as quarter
,min(week_beginning) as min_week_beginning
,max(week_ending) as max_week_ending
,country
,tactic
,sum(cost) as cost
,sum(cost_with_override) as cost_with_override
,sum(clicks) as clicks
,sum(visits) as visits
from base 
where week_beginning >= date_sub(current_date(), interval 2 quarter)
and extract(quarter from week_beginning) != extract(quarter from current_date())
group by 1,4,5
order by 1,2,3,4 desc
;

END
