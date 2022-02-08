create temp table ranks as (
with ranks AS (
    SELECT
        a_1.buy_visit_id,
        v.start_datetime,
        c.mapped_user_id,
        c.buyer_type, 
        c.purchase_day_number,	
        row_number() OVER (PARTITION BY a_1.buy_visit_id ORDER BY a_1.receipt_timestamp DESC, a_1.receipt_id DESC) AS row_number
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS a_1
        INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c on a_1.receipt_id = c.receipt_id
        INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` v on a_1.buy_visit_id = v.visit_id
  )
 SELECT
      buy_visit_id,
      start_datetime,
      mapped_user_id,
      buyer_type,
      purchase_day_number
    FROM ranks 
    WHERE row_number = 1) ;


/*
#conversion rate of users with and without a rakuten cashback touchpoint

with visit_stat as 
    (SELECT  u.mapped_user_id, v.visit_id, converted, buyer_type , purchase_day_number, r.start_datetime as buy_startdatetime, v.start_datetime,row_number() OVER (PARTITION BY v.visit_id ORDER BY r.start_datetime desc) AS row_number
    FROM `etsy-data-warehouse-prod.user_mart.user_profile` u
    join `etsy-data-warehouse-prod.buyatt_mart.visits` v on u.user_id = v.user_id
    join ranks r on v.start_datetime > r.start_datetime and u.mapped_user_id = r.mapped_user_id
    where v._date >= '2016-01-01' and v.canonical_region in ('US')),
rakuten_check as
    (SELECT  u.mapped_user_id, min(start_datetime) as first_visit_date
    FROM `etsy-data-warehouse-prod.user_mart.user_profile` u
    join `etsy-data-warehouse-prod.buyatt_mart.visits` v on u.user_id = v.user_id
    where v._date >= '2016-01-01'
    and second_channel = 'affiliates' and utm_content = '156708'  
    and canonical_region in ('US')
    group by 1)
select case when r.mapped_user_id is not null then 1 else 0 end as cashback_touchpoint, date_trunc(date(start_datetime), month) as date, count(visit_id) as visits, sum(converted) as converted
from  visit_stat vs
#join `etsy-data-warehouse-prod.buyatt_mart.visits` v on vs.visit_id = v.visit_id
left join rakuten_check r on vs.mapped_user_id = r.mapped_user_id and vs.start_datetime >= first_visit_date
where ( DATE_DIFF(DATE(start_datetime), DATE(buy_startdatetime), YEAR) >= 1 or (buyer_type = 'new_buyer'))
and row_number = 1  
group by 1,2
order by 1;
*/

/*
#ltv of users with and without a rakuten cashback touchpoint

with rakuten_check as
    (SELECT  u.mapped_user_id, min(start_datetime) as first_visit_date
    FROM `etsy-data-warehouse-prod.user_mart.user_profile` u
    join `etsy-data-warehouse-prod.buyatt_mart.visits` v on u.user_id = v.user_id
    where v._date >= '2016-01-01'
    and second_channel = 'affiliates' and utm_content = '156708'  
    and canonical_region in ('US')
    group by 1),
receipt_visits as 
    (SELECT distinct receipt_id, visit_id, canonical_region FROM `etsy-data-warehouse-prod.transaction_mart.transactions_visits`)    
select case when rc.mapped_user_id is not null then 1 else 0 end as cashback_touchpoint, purchase_date, count(distinct c.mapped_user_id) as users, avg(a.expectedgms52) as expectedgms52, avg(a.expectedgms104) as expectedgms104
 FROM `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c 
INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.analytics_clv` a on c.purchase_date = a.pred_date and c.mapped_user_id = a.mapped_user_id 
INNER JOIN receipt_visits rv on c.receipt_id = rv.receipt_id
INNER JOIN ranks r on rv.visit_id = r.buy_visit_id
left join rakuten_check rc on c.mapped_user_id = rc.mapped_user_id and c.purchase_date >= date(first_visit_date)
where (r.buyer_type in ('reactivated_buyer') or (r.buyer_type = 'existing_buyer' and r.purchase_day_number = 2))
and (canonical_region = 'US' or rc.mapped_user_id is not null)
group by 1, 2
*/

#pop stddev ltv

 with rakuten_check as
    (SELECT  u.mapped_user_id, min(start_datetime) as first_visit_date
    FROM `etsy-data-warehouse-prod.user_mart.user_profile` u
    join `etsy-data-warehouse-prod.buyatt_mart.visits` v on u.user_id = v.user_id
    where v._date >= '2016-01-01'
    and second_channel = 'affiliates' and utm_content = '156708'  
    and canonical_region in ('US')
    group by 1),
receipt_visits as 
    (SELECT distinct receipt_id, visit_id, canonical_region FROM `etsy-data-warehouse-prod.transaction_mart.transactions_visits`)    
select date_trunc(purchase_date,month) as purchase_month, count(distinct c.mapped_user_id) as users, avg(a.expectedgms52) as expectedgms52, avg(a.expectedgms104) as expectedgms104,
STDDEV_POP(expectedgms52) as expectedgms52_stddev, STDDEV_POP(expectedgms104) as expectedgms104_sttdev
 FROM `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c 
INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.analytics_clv` a on c.purchase_date = a.pred_date and c.mapped_user_id = a.mapped_user_id 
INNER JOIN receipt_visits rv on c.receipt_id = rv.receipt_id
INNER JOIN ranks r on rv.visit_id = r.buy_visit_id
left join rakuten_check rc on c.mapped_user_id = rc.mapped_user_id and c.purchase_date >= date(first_visit_date)
where (r.buyer_type in ('reactivated_buyer') or (r.buyer_type = 'existing_buyer' and r.purchase_day_number = 2))
and (canonical_region = 'US' or rc.mapped_user_id is not null)
group by 1;

#number of lapsed + otb US buyers

SELECT count(distinct mapped_user_id)
FROM (select *, row_number() over (partition by user_id order by purch_date desc) as rnk from `etsy-data-warehouse-prod.user_mart.user_purch_daily_analytic`) p
join `etsy-data-warehouse-prod.user_mart.user_profile` u on p.user_id = u.user_id
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv on u.user_id = tv.user_id
where (purch_day_number = 1 or days_since_last_purch >= 365)
and canonical_region = 'US'
and rnk = 1;
