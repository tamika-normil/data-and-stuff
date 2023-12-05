---- Queries related to gift card purchases

--- Share of receipts related to gift cards

create table if not exists etsy-data-warehouse-dev.tnormil.gc_receipts as 
  (select receipt_id, price, date(creation_tsz) as purchase_date, mapped_user_id, count(*) as cnt
  from etsy-data-warehouse-prod.transaction_mart.all_transactions aat
  where date(creation_tsz) >= '2020-01-01' and aat.is_gift_card = 1
  group by 1,2,3,4);

-- % of receipts that are gift card related by marketing region and device 

select date(timestamp_trunc(rg.creation_tsz, week)) as date_week, 
date(timestamp_trunc(rg.creation_tsz, quarter)) as date_qtr, 
date(timestamp_trunc(rg.creation_tsz, year)) as date_yr, 
case when extract(month from timestamp_trunc(rg.creation_tsz, quarter)) = 10 then 'Holiday' else 'Rest of year' end as timeframe,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
case when lower(v.mapped_platform_type) like '%mweb%android%' then "Android Mobile Web"
when lower(v.mapped_platform_type) like '%boe%android%'  then "Android BOE"
when lower(v.mapped_platform_type) like '%mweb%ios%' then "iOS Mobile Web"
when lower(v.mapped_platform_type) like '%boe%ios%' then "iOS BOE"
else "Desktop" end as device,
count(case when gc.receipt_id is not null then rg.receipt_id end) as is_gift_card_receipts,
count(distinct rg.receipt_id) as receipts
from etsy-data-warehouse-prod.transaction_mart.receipts_gms rg
left join etsy-data-warehouse-dev.tnormil.gc_receipts gc using (receipt_id)
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
where date(rg.creation_tsz) >= '2020-01-01'
group by 1,2,3,4,5,6;

-- Share of Purchases that GC by Region & Channel

create table if not exists etsy-data-warehouse-dev.tnormil.gc_attr_by_browser as
(select date(start_datetime) as date,
top_channel, 
second_channel,
third_channel,
utm_campaign, 
utm_medium,
marketing_region,
sum(external_source_decay_all) as receipts,
sum(case when gc.receipt_id is not null then external_source_decay_all end) as receipts_gc
from etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab
left join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id and v._date >= '2020-01-01'
left join etsy-data-warehouse-dev.tnormil.gc_receipts gc on ab.receipt_id = gc.receipt_id
where timestamp_seconds(o_visit_run_date) >= '2020-01-01'
group by 1,2,3,4,5,6,7);
  
with base_data as
(SELECT date, reporting_channel_group, sum(attributed_gms) as attributed_gms, sum(attributed_gms_adjusted_mult) as attributed_gms_adjusted_mult,
FROM `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` co
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using (top_channel, second_channel, third_channel, utm_campaign, utm_medium)
WHERE co.date >= '2020-01-01'
group by 1,2)
select date_trunc(date, month) as date_month,
 date_trunc(date, quarter) as date_quarter,
case when h.holiday is not null then 1 else 0 end as holiday,
case when marketing_region = 'US' then 'US' else 'INTL' end as us_intl,
reporting_channel_group,
sum(coalesce(receipts,0) * coalesce(safe_divide(attributed_gms_adjusted_mult,attributed_gms),1) ) as receipts,
sum(coalesce(receipts_gc,0) * coalesce(safe_divide(attributed_gms_adjusted_mult,attributed_gms),1) ) as receipts_gc
from etsy-data-warehouse-dev.tnormil.gc_attr_by_browser
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using (top_channel, second_channel, third_channel, utm_campaign, utm_medium)
left join base_data using (date, reporting_channel_group)
left join etsy-data-warehouse-dev.tnormil.holidays h using (date, marketing_region)
where marketing_region in ('US','GB','DE','FR','CA') 
group by 1,2,3,4,5
order by 2,1;

--- Buyer type distribution stuff

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
 
-- buyer type distibution by frequency 

with gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null)
select date_trunc(purchase_date,month) as month, buyer_type_new, count(distinct receipt_id) as receipts
from gms_share_buyer_type
group by 1,2
;

-- repurchase rate by buyer type frequency 

with receipt_date as 
    (select purchase_date, mapped_user_id, buyer_type, count(*)
    from receipt_data
    group by 1,2,3),
gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null),
gc_purchases as 
(select date_trunc(gc.purchase_date,month) as month, buyer_type_new as buyer_type, count(distinct gc.mapped_user_id) as buyers,
count(distinct r.mapped_user_id) as repurchase_30,
count(distinct r2.mapped_user_id) as repurchase_60,
from gms_share_buyer_type gc
left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and r.purchase_date > gc.purchase_date and r.purchase_date < date_add(gc.purchase_date, interval 30 day)
left join receipt_data r2 on gc.mapped_user_id = r2.mapped_user_id and r2.purchase_date > gc.purchase_date and r2.purchase_date < date_add(gc.purchase_date, interval 60 day)
group by 1,2),
next_receipt as 
  (select *, lag(purchase_date) over (partition by mapped_user_id order by purchase_date desc) as next_purchase_date
  from receipt_date),
all_purchases as 
(select date_trunc(purchase_date, month) as month, buyer_type,  count(distinct mapped_user_id) as buyers,
count(distinct case when next_purchase_date < date_add(purchase_date, interval 60 day) then mapped_user_id end) as repurchase_60,
count(distinct case when next_purchase_date < date_add(purchase_date, interval 30 day) then mapped_user_id end) as repurchase_30,
from next_receipt
group by 1,2)
select g.month, g.buyer_type, g.buyers, g.repurchase_30, a.buyers, a.repurchase_30
from gc_purchases g
left join all_purchases a using (month,buyer_type)

-- buyer type distibution by frequency x target gender

with gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null)
select case when country = 'United States' then 'US' else 'INTL' end as us_intl,
date_trunc(gc.purchase_date, month) as month, target_gender ,
case when buyer_type_new = 'new_buyer' then buyer_type_new else 'existing_buyer' end as buyer_type,
count(distinct gc.receipt_id) as receipts
from etsy-data-warehouse-dev.tnormil.gc_receipts gc
left join `etsy-data-warehouse-prod.rollups.buyer_basics` using (mapped_user_id)
left join gms_share_buyer_type using (receipt_id)
group by 1,2,3,4;

-- buyer type distibution by frequency x target gender (more detailed as it includes index to compare to company wide stats)

with gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null),
gc_share as 
    (select case when country = 'United States' then 'US' else 'INTL' end as us_intl,
    date_trunc(gc.purchase_date, month) as month, target_gender ,
    case when buyer_type_new in ('4_to_9x_buyer', '10plus_buyer') then 'habitual' when buyer_type_new is not null then buyer_type_new else 'unknown' end as buyer_type,
    count(distinct gc.receipt_id) as receipts
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join `etsy-data-warehouse-prod.rollups.buyer_basics` using (mapped_user_id)
    left join gms_share_buyer_type using (receipt_id)
    group by 1,2,3,4),
all_share as 
    (select case when country = 'United States' then 'US' else 'INTL' end as us_intl,
    date_trunc(purchase_date, month) as month, target_gender ,
    case when buyer_type in ('4_to_9x_buyer', '10plus_buyer') then 'habitual' when buyer_type is not null then buyer_type else 'unknown' end as buyer_type,
    count(distinct receipt_id) as receipts
    from receipt_data 
    left join `etsy-data-warehouse-prod.rollups.buyer_basics` using (mapped_user_id)
    group by 1,2,3,4)
select g.*, a.*
from gc_share g
left join all_share a using (us_intl, month,  target_gender,  buyer_type)
;

-- buyer type distibution by frequency x age

with gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null)
select case when country = 'United States' then 'US' else 'INTL' end as us_intl,
date_trunc(gc.purchase_date, month) as month, estimated_age ,
case when buyer_type_new = 'new_buyer' then buyer_type_new else 'existing_buyer' end as buyer_type,
count(distinct gc.receipt_id) as receipts
from etsy-data-warehouse-dev.tnormil.gc_receipts gc
left join `etsy-data-warehouse-prod.rollups.buyer_basics` using (mapped_user_id)
left join gms_share_buyer_type using (receipt_id)
group by 1,2,3,4;

-- buyer type distibution by frequency x age (more detailed as it includes index to compare to company wide stats)

with gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null),
gc_share as 
    (select case when country = 'United States' then 'US' else 'INTL' end as us_intl,
    date_trunc(gc.purchase_date, month) as month, estimated_age ,
    case when buyer_type_new in ('4_to_9x_buyer', '10plus_buyer') then 'habitual' when buyer_type_new is not null then buyer_type_new else 'unknown' end as buyer_type,
    count(distinct gc.receipt_id) as receipts
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join `etsy-data-warehouse-prod.rollups.buyer_basics` using (mapped_user_id)
    left join gms_share_buyer_type using (receipt_id)
    group by 1,2,3,4),
all_share as 
    (select case when country = 'United States' then 'US' else 'INTL' end as us_intl,
    date_trunc(purchase_date, month) as month, estimated_age ,
    case when buyer_type in ('4_to_9x_buyer', '10plus_buyer') then 'habitual' when buyer_type is not null then buyer_type else 'unknown' end as buyer_type,
    count(distinct receipt_id) as receipts
    from receipt_data 
    left join `etsy-data-warehouse-prod.rollups.buyer_basics` using (mapped_user_id)
    group by 1,2,3,4)
select g.*, a.*
from gc_share g
left join all_share a using (us_intl, month, estimated_age,  buyer_type)
;

--- AOV stuff

-- Past Year Distribution of Gift Cards Purchased by Original Gift Card Value & Key Market

SELECT amount_start/100 as value,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
count(distinct aat.transaction_id) as transactions
FROM
`etsy-data-warehouse-prod.etsy_payments.giftcards` as a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` aat on a.purchase_transaction_id = aat.transaction_id
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
WHERE a.type = 'STANDARD'
and date(aat.creation_tsz) >= date_sub(current_date - 1, interval 1 year)
and transaction_live = 1
group by 1, 2;

-- Avg transaction value of GC purchase by region over time

SELECT date_trunc(date(aat.creation_tsz), month) as date_month,
date_trunc(date(aat.creation_tsz), year) as date_year,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
sum(amount_start/100) as sum_value,
count(distinct aat.transaction_id) as transactions,
sum(aat.quantity) as quantity
FROM
`etsy-data-warehouse-prod.etsy_payments.giftcards` as a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` aat on a.purchase_transaction_id = aat.transaction_id
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
WHERE a.type = 'STANDARD'
and date(aat.creation_tsz) >= '2021-01-01'
and transaction_live = 1
group by 1, 2, 3;

-- Median transaction value of GC purchase by region over time
/*
SELECT distinct date_trunc(date(aat.creation_tsz), month) as date_month,
date_trunc(date(aat.creation_tsz), year) as date_year,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
PERCENTILE_DISC(aat.quantity, 0.5) OVER(partition by date_trunc(date(aat.creation_tsz), month), case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end) AS median_quantity,
PERCENTILE_DISC(amount_start/100, 0.5) OVER(partition by date_trunc(date(aat.creation_tsz), month), case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end) AS median_quantity
FROM
`etsy-data-warehouse-prod.etsy_payments.giftcards` as a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` aat on a.purchase_transaction_id = aat.transaction_id
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
WHERE a.type = 'STANDARD'
and date(aat.creation_tsz) >= '2021-01-01'
and transaction_live = 1;
*/

-- When do gift card purchases peak before holiday?
-- Peaks are estimated by identifying days where 2 of 3 days before or after the respective day have receipts share dedicated to gift card purchases above benchmarks

with relevant_holiday as 
(select *
from etsy-data-warehouse-dev.tnormil.holidays
where holiday in ("Father's Day",
"Mother's Day",
"Christmas Day",
"Valentine's Day",
"Boxing Day")),
daily as
(select v.marketing_region,
date(creation_tsz) as purchase_date,
holiday,
date_diff(h.date,date(creation_tsz), day) as days_to_holiday,
count(distinct rg.receipt_id) as receipts,
count(distinct case when rg.is_gift_card = 1 then rg.receipt_id end) as gc_receipts
from etsy-data-warehouse-prod.transaction_mart.receipts_gms rg
left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
left join relevant_holiday h on date(creation_tsz) >= date_sub(h.date, interval 30 day) 
and date(creation_tsz) <= date_add(h.date, interval 30 day)  
and v.marketing_region = h.marketing_region
where v.marketing_region in ('US','CA','FR','GB','DE')
group by 1,2,3,4),
non_holiday_history as 
(select distinct purchase_date, marketing_region, 
sum(IF(holiday is null, gc_receipts, 0)) OVER (partition by marketing_region ORDER BY purchase_date asc ROWS BETWEEN 180 PRECEDING AND current row) as gc_receipts,
sum(IF(holiday is null, receipts, 0)) OVER (partition by marketing_region ORDER BY purchase_date asc ROWS BETWEEN 180 PRECEDING AND current row) as receipts,
from daily),
daily_history as 
(select d.*, 
h.gc_receipts as benchmark_gc_receipts,
h.receipts as benchmark_receipts,
safe_divide(h.gc_receipts, h.receipts) as benchmark_share,
safe_divide(d.gc_receipts, d.receipts) as share_gc,
COUNTIF( safe_divide(d.gc_receipts, d.receipts) > safe_divide(h.gc_receipts, h.receipts)*1.2 ) OVER (partition by d.marketing_region, d.holiday ORDER BY d.purchase_date desc ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as historical_peaks_before,
COUNTIF( safe_divide(d.gc_receipts, d.receipts) > safe_divide(h.gc_receipts, h.receipts)*1.2  ) OVER (partition by d.marketing_region, d.holiday ORDER BY d.purchase_date asc ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as historical_peaks_after,
from daily d
left join (select purchase_date, marketing_region, gc_receipts, receipts, row_number() over (partition by purchase_date, marketing_region order by safe_divide(gc_receipts, receipts) desc) as rnk
from non_holiday_history qualify rnk = 1) h on d.purchase_date = h.purchase_date and d.marketing_region = h.marketing_region)
select *
from daily_history
where holiday is not null
order by marketing_region, holiday, purchase_date asc;

-- When do gift card purchases peak before holiday?
-- Another approach, comparing 7 vs 60 day average

/*

with relevant_holiday as 
(select *, lag(date) over (partition by marketing_region order by date asc) as last_holiday
from etsy-data-warehouse-dev.tnormil.holidays
where holiday in ("Father's Day",
"Mother's Day",
"Christmas Day",
"Valentine's Day",
"Boxing Day")),
daily as
(select v.marketing_region,
date(creation_tsz) as purchase_date,
holiday,
date_diff(h.date,date(creation_tsz), day) as days_to_holiday,
count(distinct rg.receipt_id) as receipts,
count(distinct case when rg.is_gift_card = 1 then rg.receipt_id end) as gc_receipts
from etsy-data-warehouse-prod.transaction_mart.receipts_gms rg
#left join etsy-data-warehouse-dev.tnormil.gc_receipts gc using (receipt_id)
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
left join relevant_holiday h on date(creation_tsz) >= date_sub(h.date, interval 30 day) 
#and date(creation_tsz) >= last_holiday
and date(creation_tsz) <= date_add(h.date, interval 14 day)  
and v.marketing_region = h.marketing_region
where v.marketing_region in ('US','CA','FR','GB','DE')
group by 1,2,3,4),
daily_history as 
(select d.*, 
SUM(gc_receipts)
  OVER ( partition by holiday, marketing_region ORDER BY purchase_date asc ROWS BETWEEN 60 Preceding AND Current row) AS gc_receipts_60day,
SUM(receipts)
  OVER ( partition by holiday, marketing_region ORDER BY purchase_date asc ROWS BETWEEN 60 Preceding AND Current row) AS receipts_60day,
SUM(gc_receipts)
  OVER ( partition by holiday, marketing_region ORDER BY purchase_date asc ROWS BETWEEN 7 Preceding AND Current row) AS gc_receipts_7day,
SUM(receipts)
  OVER ( partition by holiday, marketing_region ORDER BY purchase_date asc ROWS BETWEEN 7 Preceding AND Current row) AS receipts_7day,
avg( safe_divide(gc_receipts,receipts ) )
  OVER ( partition by holiday, marketing_region ORDER BY purchase_date asc ROWS BETWEEN 60 Preceding AND Current row) AS gc_receipts_share_60day,
avg( safe_divide(gc_receipts,receipts ) )
  OVER ( partition by holiday, marketing_region ORDER BY purchase_date asc ROWS BETWEEN 7 Preceding AND Current row) AS gc_receipts_share_7day,
from daily d)
select *
from daily_history
where holiday is not null
order by marketing_region, purchase_date asc;

*/
