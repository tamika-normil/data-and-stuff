create temp table buyer_type_labels as 
(SELECT 'zero_time_buyer' as buyer_type, '1. zero time buyer' as buyer_type_label
UNION ALL SELECT 'reactivated_buyer', '2. reactivated_buyer'
UNION ALL SELECT 'new_buyer', '3. new_buyer'
UNION ALL SELECT '2x_buyer', '4. 2x_buyer'
UNION ALL SELECT '3x_buyer', '5. 3x_buyer'
UNION ALL SELECT '4_to_9x_buyer', '6. 4_to_9x_buyer'
UNION ALL SELECT '10plus_buyer', '7. 10plus_buyer');

create temp table buyer_type_risk as 
(Select '5. 3x_buyer' as buyer_type,'2. reactivated_buyer' as optin_buyer_type,-231685.834832114 as value, 'low risk' as risk UNION ALL 
Select '5. 3x_buyer' ,'5. 3x_buyer' ,0, 'low risk' UNION ALL 
Select '5. 3x_buyer' ,'4. 2x_buyer' ,14696.1582067437, 'low risk' UNION ALL 
Select '5. 3x_buyer' ,'1. zero_time_buyer' ,374107.775460455, 'low risk' UNION ALL 
Select '5. 3x_buyer' ,'3. new_buyer' ,491938.263578918, 'low risk' UNION ALL 
Select '2. reactivated_buyer' ,'2. reactivated_buyer' ,0, 'low risk' UNION ALL 
Select '2. reactivated_buyer' ,'1. zero_time_buyer' ,495575.618442782, 'low risk' UNION ALL 
Select '2. reactivated_buyer' ,'3. new_buyer' ,799213.22793096, 'mid risky' UNION ALL 
Select '4. 2x_buyer' ,'2. reactivated_buyer' ,-322.473207361953, 'low risk' UNION ALL 
Select '4. 2x_buyer' ,'4. 2x_buyer' ,0, 'low risk' UNION ALL 
Select '4. 2x_buyer' ,'1. zero_time_buyer' ,517095.437046857, 'low risk' UNION ALL 
Select '4. 2x_buyer' ,'3. new_buyer' ,1073761.50106857, 'mid risky' UNION ALL 
Select '6. 4_to_9x_buyer' ,'6. 4_to_9x_buyer' ,0, 'low risk' UNION ALL 
Select '6. 4_to_9x_buyer' ,'4. 2x_buyer' ,377709.552199404, 'low risk' UNION ALL 
Select '6. 4_to_9x_buyer' ,'5. 3x_buyer' ,735072.651300795, 'mid risky' UNION ALL 
Select '6. 4_to_9x_buyer' ,'2. reactivated_buyer' ,961107.09164714, 'mid risky' UNION ALL 
Select '6. 4_to_9x_buyer' ,'3. new_buyer' ,1258558.06524916, 'mid risky' UNION ALL 
Select '6. 4_to_9x_buyer' ,'1. zero_time_buyer' ,1852077.59369205, 'high risk' UNION ALL 
Select '7. 10plus_buyer' ,'7. 10plus_buyer' ,0, 'low risk' UNION ALL 
Select '7. 10plus_buyer' ,'4. 2x_buyer' ,197834.697576754, 'low risk' UNION ALL 
Select '7. 10plus_buyer' ,'5. 3x_buyer' ,335616.135274985, 'low risk' UNION ALL 
Select '7. 10plus_buyer' ,'3. new_buyer' ,748747.337906356, 'mid risky' UNION ALL 
Select '7. 10plus_buyer' ,'6. 4_to_9x_buyer' ,1271237.15271009, 'mid risky' UNION ALL 
Select '7. 10plus_buyer' ,'1. zero_time_buyer' ,2152876.84066547, 'high risk' UNION ALL 
Select '7. 10plus_buyer' ,'2. reactivated_buyer' ,7315430.72075589, 'extremely risky');

-- add buyer type labels to actual receipt data over time

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
    WHERE extract(YEAR from CAST(purchase_date as DATETIME)) >= 2014;

-- find the latest status of each buyer. if a user hasn't made a purchase in the past year, he/she is now lapsed

create temp table last_purchase as 
with x as 
(select mapped_user_id, purchase_date,
CASE
  WHEN date_diff(current_date, purchase_date, day) >= 365 then 'reactivated_buyer'
   WHEN buyer_type = 'new_buyer' THEN 'new_buyer'
   WHEN purchase_day_number = 2
   AND date_diff(current_date, purchase_date, day) < 365 THEN '2x_buyer'
   WHEN purchase_day_number = 3
   AND date_diff(current_date, purchase_date, day) < 365 THEN '3x_buyer'
   WHEN purchase_day_number >= 4 and purchase_day_number<= 9
   AND date_diff(current_date, purchase_date, day) < 365 THEN '4_to_9x_buyer'
   WHEN purchase_day_number >= 10
   AND date_diff(current_date, purchase_date, day) < 365 THEN '10plus_buyer'
  ELSE 'zero_time_buyer'
END AS buyer_type,
purchase_day_number,
date_diff(current_date, purchase_date, day) as days_since_last_purchase,
row_number() over (partition by mapped_user_id order by purchase_date desc) as purchase_rank
FROM  `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv`
WHERE extract(YEAR from CAST(purchase_date as DATETIME)) >= 2018
qualify purchase_rank = 1)
select x.*, b.buyer_type_label
from x
left join buyer_type_labels b using (buyer_type)
 ;

/*
select *
from last_purchase
where buyer_type = '8. other'
limit 100;
*/

-- number of devices and opted out share of devices by lookback window

select case when unique_browsers >= 100 then 100 else unique_browsers end as unique_browsers, 
attribution_window_number_of_day as time_window, 
safe_divide(unique_opt_out_browsers, unique_browsers) as share_opt_out,
count(distinct mapped_user_id) as users 
from etsy-data-warehouse-dev.tnormil.opp_sizing_liveramp
where mapped_user_id is not null
group by 1,2,3;

-- validate opt out rate by lookback window

SELECT attribution_window_number_of_day, sum(unique_browsers) as unique_browsers, sum(unique_opt_out_browsers) as unique_opt_out_browsers
, sum(browsers) as browsers
, sum(opt_out_browsers) as opt_out_browsers
, sum(visits) as visits
, sum(opt_out_visits) as opt_out_visits
FROM etsy-data-warehouse-dev.tnormil.opp_sizing_liveramp
where mapped_user_id is not null
group by 1
order by 1 desc;

-- number of devices and opted out share of devices by latest buyer freq status

select unique_browsers, 
case when l.buyer_type is null then '1. zero time buyer' else buyer_type_label end as buyer_type, 
safe_divide(unique_opt_out_browsers, unique_browsers) as share_opt_out,
count(distinct o.mapped_user_id) as users 
from etsy-data-warehouse-dev.tnormil.opp_sizing_liveramp o
left join last_purchase l using (mapped_user_id)
where o.mapped_user_id is not null
and attribution_window_number_of_day = 1095
and unique_browsers <= 20
group by 1,2,3;


-- join actual recent data to opted in receipt data 

with dates as
(select distinct mapped_user_id, purchase_date, buyer_type
from 
receipt_data),
optin_dates as
(select distinct mapped_user_id, purchase_date, 
    CASE
        WHEN buyer_type = 'new_buyer' THEN 'new_buyer'
        WHEN purchase_days = 2
         AND buyer_type <> 'reactivated_buyer' THEN '2x_buyer'
        WHEN purchase_days = 3
         AND buyer_type <> 'reactivated_buyer' THEN '3x_buyer'
        WHEN purchase_days >= 4 and purchase_days <= 9
         AND buyer_type <> 'reactivated_buyer' THEN '4_to_9x_buyer'
        WHEN purchase_days >= 10
         AND buyer_type <> 'reactivated_buyer' THEN '10plus_buyer'
        WHEN buyer_type = 'reactivated_buyer' THEN 'reactivated_buyer'
        ELSE 'other'
      END AS buyer_type,
from etsy-data-warehouse-dev.tnormil.ltv_user_data)
select d.mapped_user_id, d.purchase_date as purchase_date, d.buyer_type as buyer_type,
l.mapped_user_id as optin_mapped_user_id,
l.purchase_date as optin_purchase_date, l.buyer_type as optin_buyer_type,
from dates d
left join optin_dates l using (mapped_user_id, purchase_date)
where l.mapped_user_id is null;

-- get most recent opted in buyer freq status for each purchase 

create temp table adjusted_receipt_data as 
(with dates as
(select r.mapped_user_id, r.purchase_date, r.buyer_type, r.purchase_day_number, sum(ltv_revenue * external_source_decay_all) as ltv_revenue, 
sum(case when v.visit_id is not null then ltv_revenue * external_source_decay_all end) paid_ltv_revenue,
sum(external_source_decay_all) as cvr, 
sum(case when v.visit_id is not null then external_source_decay_all end) paid_cvr,
from 
receipt_data r
left join  etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on r.receipt_id = ab.receipt_id
and ab.o_visit_run_date >= unix_seconds(timestamp('2021-01-01'))
left join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id and v.top_channel in ('us_paid','intl_paid')
and v._date >= '2021-01-01'
group by 1,2,3,4),
optin_dates as
  (select distinct mapped_user_id, purchase_date, 
      CASE
          WHEN buyer_type = 'new_buyer' THEN 'new_buyer'
          WHEN purchase_days = 2
          AND buyer_type <> 'reactivated_buyer' THEN '2x_buyer'
          WHEN purchase_days = 3
          AND buyer_type <> 'reactivated_buyer' THEN '3x_buyer'
          WHEN purchase_days >= 4 and purchase_days <= 9
          AND buyer_type <> 'reactivated_buyer' THEN '4_to_9x_buyer'
          WHEN purchase_days >= 10
          AND buyer_type <> 'reactivated_buyer' THEN '10plus_buyer'
          WHEN buyer_type = 'reactivated_buyer' THEN 'reactivated_buyer'
          ELSE 'other'
        END AS buyer_type,
        purchase_days, 
  from etsy-data-warehouse-dev.tnormil.ltv_user_data),
adjusted_receipt_data as 
  (select d.mapped_user_id, d.purchase_date as purchase_date, d.buyer_type as buyer_type, d.ltv_revenue, d.paid_ltv_revenue,
  d.cvr, d.paid_cvr, d.purchase_day_number as purchase_days, 
  l.mapped_user_id as optin_mapped_user_id,
  l.purchase_date as optin_purchase_date, l.buyer_type as optin_buyer_type,
  l.purchase_days as optin_purchase_days,
    MAX(l.purchase_date)
      OVER (partition BY d.mapped_user_id ORDER BY d.purchase_date desc
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
      AS optin_purchase_date_most_recent,
  from dates d
  left join optin_dates l using (mapped_user_id, purchase_date)
  --where d.mapped_user_id in (108048887, 77595767, 192287667, 12402178, 70823224, 63229192, 668480986)
  ORDER BY d.mapped_user_id, d.purchase_date)
select a.mapped_user_id, a.purchase_date as purchase_date, a.buyer_type as buyer_type, a.ltv_revenue, a.paid_ltv_revenue,
a.cvr, a.paid_cvr, a.purchase_days, 
 optin_purchase_date, optin_buyer_type, optin_purchase_days,
optin_purchase_date_most_recent,
b.purchase_days as optin_purchase_days_most_recent,
CASE
  WHEN date_diff(a.purchase_date, b.purchase_date, day) >= 365 then 'reactivated_buyer'
  WHEN date_diff(a.purchase_date, b.purchase_date, day) < 365 AND b.purchase_days IS NOT NULL THEN b.buyer_type
  else 'zero_time_buyer' END AS optin_buyer_type_most_recent,
from adjusted_receipt_data a
left join optin_dates b on a.mapped_user_id = b.mapped_user_id and a.optin_purchase_date_most_recent = b.purchase_date
ORDER BY a.mapped_user_id, a.purchase_date);

-- compare opted in buyer freq status to actual buyer freq status and aggregate paid revenue and conversions from each pair 

select date_trunc(purchase_date, year) as year, 
a.buyer_type,
a.optin_buyer_type_most_recent,
b1.buyer_type_label as buyer_type_label, 
b2.buyer_type_label as optin_buyer_type_label_most_recent,
sum(ltv_revenue) as ltv_revenue,
sum(paid_ltv_revenue) as paid_ltv_revenue,
sum(cvr) as cvr,
sum(paid_cvr) as cvr,
from adjusted_receipt_data a
left join buyer_type_labels b1 using (buyer_type)
left join buyer_type_labels b2 on a.optin_buyer_type_most_recent = b2.buyer_type
group by 1,2,3,4,5;

-- validate revenue + conversions reported by adjusted receipts data against source data for validation 

select date_trunc(r.purchase_date, year) as year, sum(attr_rev * external_source_decay_all) as ltv_revenue, 
sum(case when v.visit_id is not null then attr_rev * external_source_decay_all end) paid_ltv_revenue
from `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` r
left join  etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on r.receipt_id = ab.receipt_id
and ab.o_visit_run_date >= unix_seconds(timestamp('2021-01-01'))
left join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id and v.top_channel in ('us_paid','intl_paid')
and v._date >= '2021-01-01'
group by 1
order by 1;

create temp table last_purchase_adjusted as 
with x as 
(select mapped_user_id, purchase_date,
CASE
  WHEN date_diff(current_date, purchase_date, day) >= 365 then 'reactivated_buyer'
  WHEN date_diff(current_date, purchase_date, day) < 365 THEN buyer_type end as buyer_type,
CASE
  WHEN date_diff(current_date, optin_purchase_date_most_recent, day) >= 365 then 'reactivated_buyer'
  WHEN date_diff(current_date, optin_purchase_date_most_recent, day) < 365 then optin_buyer_type_most_recent END
  as optin_buyer_type_most_recent,  
date_diff(current_date, purchase_date, day) as days_since_last_purchase,
date_diff(current_date, optin_purchase_date_most_recent, day) as optin_days_since_last_purchase,
purchase_days, 
optin_purchase_days_most_recent as optin_purchase_days,
row_number() over (partition by mapped_user_id order by purchase_date desc) as purchase_rank
FROM adjusted_receipt_data
qualify purchase_rank = 1)
select x.*, b1.buyer_type_label, b2.buyer_type_label as optin_buyer_type_label_most_recent
from x 
left join buyer_type_labels b1 on x.buyer_type = b1.buyer_type
left join buyer_type_labels b2 on x.optin_buyer_type_most_recent = b2.buyer_type
 ;

with x as 
(select unique_browsers, 
case when l.buyer_type is null then '1. zero_time_buyer' else buyer_type_label end as buyer_type, 
case when l.optin_buyer_type_most_recent is null then '1. zero_time_buyer' else optin_buyer_type_label_most_recent end as optin_buyer_type_most_recent,
safe_divide(unique_opt_out_browsers, unique_browsers) as share_opt_out,
count(distinct o.mapped_user_id) as users,
from etsy-data-warehouse-dev.tnormil.opp_sizing_liveramp o
left join last_purchase_adjusted l using (mapped_user_id)
where o.mapped_user_id is not null
and attribution_window_number_of_day = 1095
and unique_browsers <= 20
group by 1,2,3,4)
select unique_browsers, 
x.buyer_type, 
x.optin_buyer_type_most_recent, 
risk,
share_opt_out,
sum(users) as users,
max(value) as value
from x
left join buyer_type_risk b on x.buyer_type = b.buyer_type and x.optin_buyer_type_most_recent = b.optin_buyer_type
group by 1,2,3,4,5;

select distinct buyer_type, 
optin_buyer_type_most_recent,
PERCENTILE_CONT (purchase_days, .50 IGNORE NULLS) OVER (PARTITION BY buyer_type, optin_buyer_type_most_recent) as purchase_days,
PERCENTILE_CONT (optin_purchase_days, .50 IGNORE NULLS) OVER (PARTITION BY buyer_type, optin_buyer_type_most_recent) as optin_purchase_days,
PERCENTILE_CONT (days_since_last_purchase, .50 IGNORE NULLS) OVER (PARTITION BY buyer_type, optin_buyer_type_most_recent) as days_since_last_purchase,
PERCENTILE_CONT (optin_days_since_last_purchase, .50 IGNORE NULLS) OVER (PARTITION BY buyer_type, optin_buyer_type_most_recent) as optin_days_since_last_purchase,
from last_purchase_adjusted; 
