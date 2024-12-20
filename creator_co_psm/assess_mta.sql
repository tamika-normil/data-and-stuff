-- (1) Is this channel typically the first, last, or middle touch?

begin 

DECLARE start_date, end_date, attribution_start_date DATE;
SET start_date = DATE "2024-01-01";
SET end_date = current_date();
SET attribution_start_date = DATE_ADD(start_date, INTERVAL -30 DAY);

with all_visits as (
  select 
  visit_id,
  start_datetime,
  top_channel,
  second_channel,
  third_channel,
  utm_campaign,
  utm_medium,
  utm_content
  from `etsy-data-warehouse-prod.buyatt_mart.visits`
  where partition_key>=UNIX_SECONDS(CAST(attribution_start_date as TIMESTAMP))
    and partition_key<=UNIX_SECONDS(CAST(end_date as TIMESTAMP))
),
attr_visits as(
  select
  a.o_visit_id,
  a.receipt_id,
  date(a.receipt_timestamp) as date,
  a.gms*a.external_source_decay_all as attr_gms,
  v.start_datetime,
  ROW_NUMBER() OVER(PARTITION BY a.receipt_id ORDER BY v.start_datetime asc) journey_step,
  v.top_channel,
  v.second_channel,
  v.third_channel,
  v.utm_campaign,
  v.utm_medium,
  case when p.publisher_id is not null then p.tactic else cd.reporting_channel_group end as reporting_channel_group, 
  FROM `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` a
  left join all_visits v on v.visit_id = a.o_visit_id
  left join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd 
    on v.top_channel = cd.top_channel 
    and v.second_channel = cd.second_channel 
    and v.third_channel = cd.third_channel 
    and v.utm_campaign = cd.utm_campaign 
    and v.utm_medium = cd.utm_medium 
  left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic p
    on v.utm_content = p.publisher_id and p.tactic in ('Social Creator Co - CreatorIQ','Influencer Subnetwork') and v.second_channel = 'affiliates'
  where DATE(receipt_timestamp)>=start_date 
    and DATE(receipt_timestamp)<=end_date
    and cd. reporting_channel_group <> 'Internal' 
  order by 2,3
),
journey_steps as(
select 
receipt_id,
MAX(journey_step) as max_step
from attr_visits
group by 1
),
c as (
  select 
    av.receipt_id,
    av.date,
    av.reporting_channel_group,
    av.attr_gms,
    av.journey_step,
    j.max_step,
    case when av.journey_step = 1 then 1 else 0 end as first_touch,
    case when av.journey_step = j.max_step then 1 else 0 end as last_touch,
    case when (av.journey_step != j.max_step and av.journey_step != 1) then 1 else 0 end as mid_touch,
        case when av.journey_step = 1 then av.attr_gms else 0 end as first_touch_gms,
    case when av.journey_step = j.max_step then av.attr_gms else 0 end as last_touch_gms,
    case when (av.journey_step != j.max_step and av.journey_step != 1) then av.attr_gms else 0 end as mid_touch_gms,
  from attr_visits av
  left join journey_steps j on j.receipt_id=av.receipt_id
),
agg_trans as (
select 
  receipt_id,
  date,
  reporting_channel_group,
  sum(first_touch) as first_touch,
  sum(mid_touch) as mid_touch,
  sum(last_touch) as last_touch,
  case when sum(mid_touch) >1 then 1 else sum(mid_touch) end as trunc_middle,
  sum(first_touch_gms) as first_touch_gms,
  sum(mid_touch_gms) as mid_touch_gms,
  sum(last_touch_gms) as last_touch_gms,
  from c  
group by 1,2,3
)
select 
date,
reporting_channel_group,
sum(first_touch) as first_touch, 
sum(mid_touch) as mid_touches, 
sum(last_touch) as last_touch, 
sum(trunc_middle) as truncated_mid,
sum(first_touch_gms) as first_touch_gms,
sum(mid_touch_gms) as mid_touch_gms,
sum(last_touch_gms) as last_touch_gms,
from agg_trans
group by 1,2;

end;

-- (2) How is credit distributed by channel?
  
begin

-- limit data to paid receipts 

create temp table receipts_base as
  (select ar.receipt_id, creation_tsz as receipt_timestamp, case when p.publisher_id is not null then p.tactic else c.reporting_channel_group end as reporting_channel_group, count(*) as row_cnt
  from etsy-data-warehouse-prod.transaction_mart.all_receipts ar 
  join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on ar.receipt_id = ab.receipt_id 
  join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id and ab.o_visit_run_date = v.run_date
  left join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` c using 
  (utm_campaign,
  utm_medium,
  top_channel,
  second_channel,
  third_channel)
   left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic p
        on v.utm_content = p.publisher_id and p.tactic in ('Social Creator Co - CreatorIQ','Influencer Subnetwork') and v.second_channel = 'affiliates'
  where v._date >= '2024-01-01'
  and o_visit_run_date >= unix_seconds(timestamp('2024-01-01'))
  and top_channel in ('us_paid','intl_paid')
  group by 1,2,3);

-- summarize attribution credit by channel per receipt 

create temp table credit_deet as
with base as 
(select receipt_id, receipt_timestamp, count(*) as row_cnt 
from receipts_base group by 1,2)
  (select date(r.receipt_timestamp) as date, r.receipt_id, case when p.publisher_id is not null then p.tactic else c.reporting_channel_group end as  reporting_channel_group_w_credit, sum(external_source_decay_all) as attributed_receipts, max(start_datetime) as last_visit
  from base r
  join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on r.receipt_id = ab.receipt_id
  join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id and ab.o_visit_run_date = v.run_date
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using (top_channel, second_channel,third_channel, utm_medium, utm_campaign)
  left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic p
    on v.utm_content = p.publisher_id and p.tactic in ('Social Creator Co - CreatorIQ','Influencer Subnetwork') and v.second_channel = 'affiliates'
  where v._date >= '2024-01-01'
  and o_visit_run_date >= unix_seconds(timestamp('2024-01-01'))
    group by 1,2,3);

create or replace temp table final as
with credit_base as 
(select *, Row_number( ) over (partition by  receipt_id order by last_visit desc) as rnk
from credit_deet ),
base as (select c.*, r.reporting_channel_group
from receipts_base r
join credit_base c using (receipt_id))
  
-- avg and median credit per reporting channel and year

with avgg as 
(SELECT date_trunc(date,  year)  year, reporting_channel_group, reporting_channel_group_w_credit, avg( attributed_receipts ) as avg_credit,
stddev( attributed_receipts ) as std_credit, 
FROM final
where reporting_channel_group = reporting_channel_group_w_credit
group by 1,2,3), 
mediann as 
(SELECT distinct date_trunc(date, year) as year, reporting_channel_group, reporting_channel_group_w_credit,
PERCENTILE_CONT( attributed_receipts , .5) over (partition by  date_trunc(date, year), reporting_channel_group, reporting_channel_group_w_credit ) as median_credit
FROM final 
where reporting_channel_group = reporting_channel_group_w_credit)
select a.*, m.median_credit
from avgg a
left join mediann m using (year, reporting_channel_group)
order by 2,1;

-- credit share by channel 

select case when reporting_channel_group in ('Social Creator Co - CreatorIQ','Influencer Subnetwork','Paid Social','Affiliates','Display','Video') 
then reporting_channel_group
when reporting_channel_group in ('PLA', 'SEM - Brand', 'SEM - Non-Brand') then 'PLA/SEM'
else reporting_channel_group end as reporting_channel_group, reporting_channel_group_w_credit, sum( attributed_receipts ) as attributed_receipts
from final
group by 1,2;

-- top conversion paths

with base as 
(select reporting_channel_group,  receipt_id, string_AGG(reporting_channel_group_w_credit ORDER BY last_visit desc) as channel_array
from final
where rnk <= 3
group by 1,2)
select reporting_channel_group, channel_array, count(distinct receipt_id) as receipts
from final 
group by 1,2;


end;

-- (3) What is the signed in rate?

select date_trunc( date(start_datetime), month) as date, case when p.publisher_id is not null then p.tactic else c.reporting_channel_group end as reporting_channel_group,
count(distinct visit_id) as visits, count(distinct case when up.user_id is not null then visit_id end) as signed_in_visits 
  from `etsy-data-warehouse-prod.buyatt_mart.visits` v
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using (top_channel, second_channel,third_channel, utm_medium, utm_campaign)
  left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic p
      on v.utm_content = p.publisher_id and p.tactic in ('Social Creator Co - CreatorIQ','Influencer Subnetwork') and v.second_channel = 'affiliates'
  left join etsy-data-warehouse-prod.hvoc.customers_by_browser hvoc on v.browser_id = hvoc.browser_id
  left join etsy-data-warehouse-prod.user_mart.user_profile up on hvoc.user_id = up.user_id
  where date(v.start_datetime) >= DATE('2020-12-01')
  and _date >=  DATE('2020-12-01')
  and top_channel in ('us_paid','intl_paid')
group by 1,2;

-- (3) What is the avg + median # of days between visit and purchase ?

begin

CREATE OR REPLACE TEMPORARY TABLE receipt_data
  AS SELECT
      receipt_id,
      mapped_user_id,
      purchase_date,
      purchase_day_number,
      coalesce(days_since_last_purch, 0) AS days_since_last_purch,
      CASE
        WHEN buyer_type = 'new_buyer' THEN 'new_buyer'
        WHEN purchase_day_number = 2
         AND buyer_type <> 'reactivated_buyer' THEN '2x_buyer'
        WHEN purchase_day_number = 3
         AND buyer_type <> 'reactivated_buyer' THEN '3x_buyer'
        WHEN purchase_day_number >= 4 and purchase_day_number<= 9
         AND buyer_type <> 'reactivated_buyer' THEN '4_to_9x_buyer'
        WHEN purchase_day_number >= 10
         AND buyer_type <> 'reactivated_buyer' THEN '10plus_buyer'
        WHEN buyer_type = 'reactivated_buyer' THEN 'reactivated_buyer'
        ELSE 'other'
      END AS buyer_type,
      recency,
      day_percent,
      attr_rev AS ltv_revenue,
      receipt_gms + (ltv_gms - day_gms) * day_percent AS ltv_gms,
      receipt_gms AS gms
    FROM
      `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv`
    WHERE extract(YEAR from CAST(purchase_date as DATETIME)) >= 2018
;

-- identify if receipt a existing either within 4 months of the original visit or before the user is exposed to another visit from the same channel

CREATE OR REPLACE TEMPORARY TABLE base_data as 
with visits_base as 
    ( select distinct v.start_datetime, case when p.publisher_id is not null then p.tactic else c.reporting_channel_group end as reporting_channel_group, top_channel,
    visit_id, mapped_user_id
    from `etsy-data-warehouse-prod.buyatt_mart.visits` v
    left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using (top_channel, second_channel,third_channel, utm_medium, utm_campaign)
    left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic p
    on v.utm_content = p.publisher_id and p.tactic in ('Social Creator Co - CreatorIQ','Influencer Subnetwork') and v.second_channel = 'affiliates'
    left join etsy-data-warehouse-prod.hvoc.customers_by_browser hvoc on v.browser_id = hvoc.browser_id
    left join etsy-data-warehouse-prod.user_mart.user_profile up on hvoc.user_id = up.user_id
    where date(v.start_datetime) >= DATE('2020-12-01')
    and _date >=  DATE('2020-12-01')
    and top_channel in ('us_paid','intl_paid')),
visits_base_adjust as 
    (select vb.*, lag(start_datetime) over (partition by  mapped_user_id, reporting_channel_group order by start_datetime asc) as next_visit 
    from visits_base vb),
base as 
    (select date(v.start_datetime) as date, reporting_channel_group,top_channel,
    visit_id, creation_tsz as receipt_timestamp, receipt_id, row_number() over (partition by visit_id order by creation_tsz asc) as rnk
    from visits_base_adjust v
    left join etsy-data-warehouse-prod.transaction_mart.all_receipts ar on v.mapped_user_id = ar.mapped_user_id
    and creation_tsz > start_datetime and date(creation_tsz) < least( date_add(date(start_datetime), interval 4 month), coalesce(date(next_visit), '2030-01-01') )
    qualify rnk = 1 or rnk is null)
select b.*, r.buyer_type, days_since_last_purch
from base b
left join receipt_data r using (receipt_id);

-- monthly avg + median days to purchase

with avgg as 
(SELECT date_trunc(date,  month) as month, reporting_channel_group,count(visit_id) as visits, avg( date_diff(date(receipt_timestamp),date, day) ) as avg_days_to_purchase, 
stddev( date_diff(date(receipt_timestamp),date, day) )  as std_days_to_purchase
FROM base_data
where rnk is not null
group by 1,2), 
mediann as 
(SELECT distinct date_trunc(date,  month) as month, reporting_channel_group, PERCENTILE_CONT( date_diff(date(receipt_timestamp),date, day) , .5) over (partition by  date_trunc(date,  month), reporting_channel_group ) as median_days_to_purchase
FROM base_data
where rnk is not null)
select a.*, m.median_days_to_purchase
from avgg a
left join mediann m using (month, reporting_channel_group)
order by 2,1;

-- yearly avg + median days to purchase

with avgg as 
(SELECT date_trunc(date, year) as year, reporting_channel_group,count(visit_id) as visits, avg( date_diff(date(receipt_timestamp),date, day) ) as avg_days_to_purchase, 
stddev( date_diff(date(receipt_timestamp),date, day) )  as std_days_to_purchase, 
FROM base_data
where rnk is not null
group by 1,2), 
mediann as 
(SELECT distinct date_trunc(date, year) as year, reporting_channel_group, PERCENTILE_CONT( date_diff(date(receipt_timestamp),date, day) , .5) over (partition by  date_trunc(date,  year), reporting_channel_group ) as median_days_to_purchase
FROM base_data
where rnk is not null)
select a.*, m.median_days_to_purchase
from avgg a
left join mediann m using (year, reporting_channel_group)
order by 2,1;

end;
