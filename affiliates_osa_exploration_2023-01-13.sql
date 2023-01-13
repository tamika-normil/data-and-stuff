#Affiliates 

begin

DECLARE start_dt datetime;
SET start_dt =  '2021-01-01';

#96% of osa eligible events are from the view listing event
SELECT  landing_event, count(distinct visit_id) as visits
FROM `etsy-data-warehouse-prod.rollups.osa_click_to_visit_join` o
join `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v using (visit_id)
where _date >= '2022-01-01'
and run_date >= unix_seconds('2022-01-01')
group by 1;

#What % of Affiliate links drive to product pages versus market/search/EP pages?
select timestamp_trunc(start_datetime, month) as month, top_channel, case when landing_event in ('view_listing') then 1 else 0 end as osa_eligible, count(*) as visits
from `etsy-data-warehouse-prod.buyatt_mart.visits_vw` 
where second_channel = 'affiliates'
and _date >= '2021-01-01'
and run_date >= unix_seconds('2021-01-01')
group by 1, 2, 3
order by 2,1,3 desc;

#What % of Affiliate sales are OSA sellers vs non-OSA sellers?
#What is est chargeability?
CREATE OR REPLACE TEMPORARY TABLE listing_views_rank as 
    (select a.visit_id, 
    listing_id, 
    sequence_number,
    seller_user_id, 
    ROW_NUMBER() OVER (PARTITION BY a.visit_id order by sequence_number asc) AS view_listing_no
    from 
    `etsy-data-warehouse-prod.analytics.listing_views` a
    join `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v using (visit_id)
    where a._date >= '2021-01-01'
    and date(timestamp_seconds(a.run_date)) >= '2021-01-01'
    and date(timestamp_seconds(a.run_date)) <= current_date - 1
    and v._date >= '2021-01-01'
    and v.run_date >= unix_seconds('2021-01-01')
    and second_channel = 'affiliates'
    and landing_event in ('view_listing'));

CREATE OR REPLACE TEMPORARY TABLE listing_views
    AS (SELECT distinct
    DATE(v.start_datetime) AS date,
    v.run_date,
    v.visit_id,
    lv.listing_id,
    lv.seller_user_id,
    v.top_channel,
    v.second_channel,
    v.third_channel,   
    v.marketing_region,
    v.utm_content,
    v.converted,
    o.seller_opt_in_status
    FROM listing_views_rank lv
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` AS v using (visit_id)
    left join etsy-data-warehouse-prod.rollups.offsite_ads_marketing o on lv.seller_user_id = o.user_id
    WHERE lv.view_listing_no = 1
    and v._date >= '2021-01-01'
    and v.run_date >= unix_seconds('2021-01-01')); 

CREATE OR REPLACE TEMPORARY TABLE listing_visits
    AS (SELECT date,
    v.top_channel,
    v.second_channel,
    v.third_channel,   
    v.marketing_region,
    v.utm_content,
    count(DISTINCT v.visit_id) AS visits,
    count(DISTINCT case when seller_opt_in_status = 1 then v.visit_id end) AS seller_opt_in_visits,
    count(DISTINCT CASE WHEN v.converted = 1 THEN v.visit_id END) AS in_session_converted_visits,
    FROM  listing_views v
    group by 1,2,3,4,5,6); 

CREATE OR REPLACE TEMPORARY table in_session_perf_listings_same_shop AS
    (SELECT
    v.visit_id,
    max(CASE WHEN al.transaction_id IS NOT NULL then 1 else 0 end) as same_shop_ind
    FROM listing_views  AS v
    INNER JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` AS tv ON v.visit_id = tv.visit_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` AS al ON tv.transaction_id = al.transaction_id AND v.seller_user_id = al.seller_user_id
    GROUP BY 1);  

CREATE OR REPLACE TEMPORARY TABLE in_session_perf_listings AS
    (SELECT
    v.date,
    v.top_channel,
    v.second_channel,
    v.third_channel,   
    v.marketing_region,
    v.utm_content,
    count(DISTINCT r.receipt_id) AS in_session_receipts,
    count(DISTINCT r.receipt_group_id) AS in_session_receipt_groups,
    sum(tt.gms_gross) AS in_session_gms_gross,
    sum(tt.gms_net) AS in_session_gms_net,
    count(DISTINCT r.receipt_id) AS in_session_receipts_listing,
    count(DISTINCT CASE
    WHEN al.same_shop_ind = 1 THEN r.receipt_group_id END) AS in_session_receipt_groups_listing,
    sum(CASE
    WHEN al.same_shop_ind = 1 THEN tt.gms_gross END) AS in_session_gms_gross_listing,
    sum(CASE
    WHEN al.same_shop_ind = 1 THEN tt.gms_net END) AS in_session_gms_net_listing
    FROM
    listing_views  AS v
    INNER JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` AS tv ON v.visit_id = tv.visit_id
    INNER JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` AS tt ON tv.transaction_id = tt.transaction_id
    INNER JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` AS r ON tt.receipt_id = r.receipt_id
    LEFT OUTER JOIN in_session_perf_listings_same_shop al on v.visit_id = al.visit_id
    WHERE DATE(tv.start_datetime) >= start_dt
    AND DATE(tv.start_datetime) <= current_date - 1
    and seller_opt_in_status = 1
    GROUP BY 1, 2, 3, 4, 5, 6);

CREATE OR REPLACE TEMPORARY table attributed_perf_listings_same_shop AS
    (SELECT
    v.visit_id,
    max(CASE WHEN `at`.receipt_id IS NOT NULL then 1 else 0 end) as same_shop_ind
    FROM listing_views  AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    AND DATE(timestamp_seconds(b.o_visit_run_date)) >= DATE_SUB(start_dt, INTERVAL 1 MONTH)
    AND DATE(timestamp_seconds(b.o_visit_run_date)) <= current_date - 1
    LEFT OUTER JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` AS `at` ON b.receipt_id = `at`.receipt_id AND v.seller_user_id = `at`.seller_user_id
    GROUP BY 1);  

CREATE OR REPLACE TEMPORARY table attributed_perf_listings AS
    (SELECT
    v.date,
    v.top_channel,
    v.second_channel,
    v.third_channel,   
    v.marketing_region,
    v.utm_content,
    sum(b.external_source_decay_all) AS attr_receipt,
    sum(b.external_source_decay_all * b.gms) AS attr_gms,
    sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,
    sum(CASE
    WHEN ss.same_shop_ind = 1 THEN b.external_source_decay_all
    ELSE CAST(NULL as FLOAT64)
    END) AS attr_receipt_listing,
    sum(CASE
    WHEN ss.same_shop_ind = 1 THEN b.external_source_decay_all * b.gms
    ELSE CAST(NULL as FLOAT64)
    END) AS attr_gms_listing,
    sum(CASE
    WHEN ss.same_shop_ind = 1 THEN b.external_source_decay_all * c.attr_rev
    ELSE CAST(NULL as FLOAT64)
    END) AS attr_rev_listing
    FROM listing_views  AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    AND DATE(timestamp_seconds(b.o_visit_run_date)) >= DATE_SUB(start_dt, INTERVAL 1 MONTH)
    AND DATE(timestamp_seconds(b.o_visit_run_date)) <= current_date - 1
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` AS c ON b.receipt_id = c.receipt_id
    LEFT OUTER JOIN attributed_perf_listings_same_shop ss on v.visit_id = ss.visit_id
    where seller_opt_in_status = 1
    GROUP BY 1, 2, 3, 4, 5, 6);  

CREATE OR REPLACE TEMPORARY TABLE perf_listings_affiliates AS 
    ( SELECT
    lv.date,
    lv.top_channel,
    lv.second_channel,
    lv.third_channel,
    lv.marketing_region,
    lv.utm_content,
    coalesce(sum(lv.visits), 0) as visits,
    coalesce(sum(lv.seller_opt_in_visits), 0) as seller_opt_in_visits,
    coalesce(sum(lv.in_session_converted_visits), 0) as in_session_converted_visits,
    coalesce(sum(sess.in_session_receipts), 0) AS in_session_receipts,
    coalesce(sum(sess.in_session_receipt_groups), 0) AS in_session_receipt_groups,
    coalesce(sum(sess.in_session_gms_gross), NUMERIC '0') AS in_session_gms_gross,
    coalesce(sum(sess.in_session_gms_net), NUMERIC '0') AS in_session_gms_net,
    coalesce(sum(sess.in_session_receipts_listing), 0) AS in_session_receipts_listing,
    coalesce(sum(sess.in_session_receipt_groups_listing), 0) AS in_session_receipt_groups_listing,
    coalesce(sum(sess.in_session_gms_gross_listing), NUMERIC '0') AS in_session_gms_gross_listing,
    coalesce(sum(sess.in_session_gms_net_listing), NUMERIC '0') AS in_session_gms_net_listing,
    coalesce(sum(att.attr_receipt), 0.0) AS attr_receipt,
    coalesce(sum(att.attr_gms), 0.0) AS attr_gms,
    (coalesce(sum(att.attr_rev), 0.0)) AS attr_rev,
    coalesce(sum(att.attr_receipt_listing), 0.0) AS attr_receipt_listing,
    coalesce(sum(att.attr_gms_listing), 0.0) AS attr_gms_listing,
    coalesce(sum(att.attr_rev_listing), 0.0) AS attr_rev_listing
    FROM listing_visits AS lv
    LEFT OUTER JOIN in_session_perf_listings AS sess ON lv.top_channel = sess.top_channel
     AND lv.second_channel = sess.second_channel
     AND lv.third_channel = sess.third_channel
     AND lv.marketing_region = sess.marketing_region
     AND lv.utm_content = sess.utm_content
     AND lv.date = sess.date
    LEFT OUTER JOIN attributed_perf_listings AS att ON lv.top_channel = att.top_channel
     AND lv.second_channel = att.second_channel
     AND lv.third_channel = att.third_channel
     AND lv.marketing_region = att.marketing_region
     AND lv.utm_content = att.utm_content
     AND lv.date = att.date
    group by 1, 2, 3, 4, 5, 6);

create temp table ss_receipts as
(with ss_receipts AS
    (SELECT b.receipt_id, b.receipt_timestamp, top_channel, max(gms) as gms
    FROM listing_views AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    AND DATE(timestamp_seconds(b.o_visit_run_date)) >= DATE_SUB('2021-01-01', INTERVAL 1 MONTH)
    AND DATE(timestamp_seconds(b.o_visit_run_date)) <= current_date - 1
    LEFT OUTER JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` AS `at` ON b.receipt_id = `at`.receipt_id AND v.seller_user_id = `at`.seller_user_id
    where `at`.receipt_id IS NOT NULL
    GROUP BY 1,2,3)
select date_trunc(receipt_timestamp, month) as month, top_channel, sum(gms) as gms
from ss_receipts 
group by 1,2);

with perf_listings_affiliates as
(SELECT date_trunc(date, month) as month,  top_channel, sum(visits) as visits, sum(seller_opt_in_visits) as seller_opt_in_visits, sum(attr_gms) as attr_gms, sum(attr_gms_listing) as attr_gms_listing
FROM perf_listings_affiliates
group by 1,2),
overall as 
(select date_trunc(date, month) as month, top_channel, case when landing_event in ('view_listing') then 1 else 0 end as osa_eligible, sum(attributed_gms_adjusted) as attributed_gms_adjusted, sum(visits) as visits 
from etsy-data-warehouse-prod.buyatt_rollups.channel_overview
where second_channel = 'affiliates'
and date >= '2021-01-01'
group by 1, 2, 3),
new_overall as 
(SELECT * FROM
overall
  PIVOT(sum(attributed_gms_adjusted) as attributed_gms_adjusted, sum(visits) as visits FOR osa_eligible IN (1,0)))
select *
from new_overall
left join perf_listings_affiliates using (month, top_channel)
left join ss_receipts using (month, top_channel);

END;

#Paid channels 

CREATE OR REPLACE TEMPORARY TABLE listing_views_rank as 
    (select a.visit_id, 
    listing_id, 
    sequence_number,
    seller_user_id, 
    ROW_NUMBER() OVER (PARTITION BY a.visit_id order by sequence_number asc) AS view_listing_no
    from 
    `etsy-data-warehouse-prod.analytics.listing_views` a
    join `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v using (visit_id)
    where a._date >= '2021-01-01'
    and date(timestamp_seconds(a.run_date)) >= '2021-01-01'
    and date(timestamp_seconds(a.run_date)) <= current_date - 1
    and v._date >= '2021-01-01'
    and v.run_date >= unix_seconds('2021-01-01')
    and (second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates', 'intl_css_plas'
                   ) or (second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%'))
    and landing_event in ('view_listing'));

CREATE OR REPLACE TEMPORARY TABLE listing_views
    AS (SELECT distinct
    DATE(v.start_datetime) AS date,
    v.run_date,
    v.visit_id,
    lv.listing_id,
    lv.seller_user_id,
    v.top_channel,
    v.second_channel,
    v.third_channel,   
    v.marketing_region,
    v.utm_content,
    v.converted,
    o.seller_opt_in_status
    FROM listing_views_rank lv
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` AS v using (visit_id)
    left join etsy-data-warehouse-prod.rollups.offsite_ads_marketing o on lv.seller_user_id = o.user_id
    WHERE lv.view_listing_no = 1
    and v._date >= '2021-01-01'
    and v.run_date >= unix_seconds('2021-01-01')); 
with perf_listings as
(SELECT date_trunc(date,month) month, second_channel, sum(first_page_attr_gms) as attr_gms, sum(first_page_attr_gms_listing) as attr_gms_listing
FROM `etsy-data-warehouse-prod.rollups.perf_listings_sum` 
where (second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates', 'intl_css_plas'
                   ) or (second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%'))
group by 1,2),
ss_receipts AS
    (SELECT b.receipt_id, b.receipt_timestamp, second_channel, max(gms) as gms
    FROM listing_views AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    AND DATE(timestamp_seconds(b.o_visit_run_date)) >= DATE_SUB('2021-01-01', INTERVAL 1 MONTH)
    AND DATE(timestamp_seconds(b.o_visit_run_date)) <= current_date - 1
    LEFT OUTER JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` AS `at` ON b.receipt_id = `at`.receipt_id AND v.seller_user_id = `at`.seller_user_id
    where `at`.receipt_id IS NOT NULL
    GROUP BY 1,2,3),
charge_est as 
  (select date_trunc(date(receipt_timestamp), month) month, second_channel, sum(gms) as gms
  from ss_receipts
  group by 1,2),
osa as 
(select date_trunc(visit_date,month) month, second_channel, sum(attr_gms) as attr_gms, sum(chargeable_gms) as chargeable_gms
from etsy-data-warehouse-prod.rollups.offsite_ads_chargeability
group by 1,2)
select *
from perf_listings
left join osa using (month, second_channel)
left join charge_est using (month, second_channel);
