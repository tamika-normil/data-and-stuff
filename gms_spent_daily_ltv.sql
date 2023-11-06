/*
create temp table gms_over_time as 
(with user_list as 
  (select distinct mapped_user_id, first_purchase_date
  from etsy-data-warehouse-prod.buyatt_mart.ltv_user_data
  where first_purchase_date >= '2019-01-01'),
dates as 
  (SELECT distinct date,  mapped_user_id, first_purchase_date
  FROM user_list
  left join UNNEST(GENERATE_DATE_ARRAY(first_purchase_date, date_add(first_purchase_date, interval 1 year), INTERVAL 1 day)) AS date
  where date < current_date),
user_purchases as
  (select mapped_user_id, first_purchase_date, purchase_date as date, sum(receipt_gms) as receipt_gms
  from etsy-data-warehouse-prod.buyatt_mart.ltv_user_data
  where first_purchase_date >= '2019-01-01'
  group by 1,2,3),
user_ltv as 
  (select mapped_user_id, sum(receipt_gms) as receipt_gms
  from etsy-data-warehouse-prod.buyatt_mart.ltv_user_data
  where first_purchase_date >= '2019-01-01'
  and purchase_date >= first_purchase_date and purchase_date < date_add(first_purchase_date, interval 1 year)
  group by 1)
select  d.date,  d.mapped_user_id, d.first_purchase_date, p.receipt_gms as day_gms, l.receipt_gms as ltv
from dates d
left join user_purchases p using (mapped_user_id, date)
left join user_ltv l using (mapped_user_id));
*/


select date_trunc(first_purchase_date, quarter) as qtr, date_diff(date, first_purchase_date, day) as  days_since_first_purchase, sum(day_gms) as gms_to_date, sum(ltv) as user_ltv
from etsy-bigquery-adhoc-prod._script9c8ec63f4834bf22698b3617481d00a734a2ae5b.gms_over_time
group by 1,2;
