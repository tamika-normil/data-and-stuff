# redo with data from attr_by_browser

begin

DECLARE start_dt datetime;
SET start_dt =  '2020-01-01';

CREATE OR REPLACE TEMPORARY TABLE listing_views_rank as 
    (select a.visit_id, 
    listing_id, 
    sequence_number,
    seller_user_id, 
    ROW_NUMBER() OVER (PARTITION BY a.visit_id order by sequence_number asc) AS view_listing_no
    from 
    `etsy-data-warehouse-prod.analytics.listing_views` a
    join `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v using (visit_id)
    where a._date >= '2020-01-01'
    and date(timestamp_seconds(a.run_date)) >= '2020-01-01'
    and date(timestamp_seconds(a.run_date)) <= current_date - 1
    and v._date >= '2020-01-01'
    and v.run_date >= unix_seconds('2020-01-01')
    and ((second_channel = 'affiliates'
    and landing_event in ('view_listing'))
    or second_channel in ('bing_plas','facebook_disp','pinterest_disp')) );

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
    and v._date >= '2020-01-01'
    and v.run_date >= unix_seconds('2020-01-01')); 

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

CREATE OR REPLACE TEMPORARY table attributed_perf_listings_same_shop AS
    (SELECT
    v.visit_id,
    max(CASE WHEN `at`.receipt_id IS NOT NULL then 1 else 0 end) as same_shop_ind
    FROM listing_views  AS v
    INNER JOIN `etsy-data-warehouse-dev.tnormil.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
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
    WHEN ss.same_shop_ind = 1 THEN b.paid_last_click_all
    ELSE CAST(NULL as FLOAT64)
    END) AS attr_receipt_listing,
    sum(CASE
    WHEN ss.same_shop_ind = 1 THEN b.paid_last_click_all * b.gms
    ELSE CAST(NULL as FLOAT64)
    END) AS attr_gms_listing,
    sum(CASE
    WHEN ss.same_shop_ind = 1 THEN b.paid_last_click_all * c.attr_rev
    ELSE CAST(NULL as FLOAT64)
    END) AS attr_rev_listing
    FROM listing_views  AS v
    INNER JOIN `etsy-data-warehouse-dev.tnormil.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
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
    coalesce(sum(att.attr_receipt), 0.0) AS attr_receipt,
    coalesce(sum(att.attr_gms), 0.0) AS attr_gms,
    (coalesce(sum(att.attr_rev), 0.0)) AS attr_rev,
    coalesce(sum(att.attr_receipt_listing), 0.0) AS attr_receipt_listing,
    coalesce(sum(att.attr_gms_listing), 0.0) AS attr_gms_listing,
    coalesce(sum(att.attr_rev_listing), 0.0) AS attr_rev_listing
    FROM listing_visits AS lv
    LEFT OUTER JOIN attributed_perf_listings AS att ON lv.top_channel = att.top_channel
     AND lv.second_channel = att.second_channel
     AND lv.third_channel = att.third_channel
     AND lv.marketing_region = att.marketing_region
     AND lv.utm_content = att.utm_content
     AND lv.date = att.date
    group by 1, 2, 3, 4, 5, 6);

  end;
  
with perf_listings_affiliates as 
(SELECT date_trunc(date, week) as week, second_channel, sum(attr_gms) as attr_gms, sum(attr_gms_listing) as attr_gms_listing, sum(case when is_seller = 0 then attr_gms end) as attr_gms_nonseller,
sum(case when is_seller = 0 then attr_gms_listing end) as attr_gms_listing_nonseller,
FROM etsy-bigquery-adhoc-prod._scriptfc5a254eb49478159302a338459b335e1af283b9.perf_listings_affiliates p
left join etsy-data-warehouse-dev.static.affiliates_publisher_by_tactic ap on p.utm_content = ap.publisher_id
group by 1,2
order by 1 desc),
osa as
(select date_trunc(visit_date, week) as week, second_channel, sum(chargeable_gms) as chargeable_gms, sum(attr_gms) as attr_gms
from etsy-data-warehouse-prod.rollups.offsite_ads_chargeability
where second_channel in ('bing_plas','facebook_disp','pinterest_disp')
group by 1,2)
select week, second_channel, p.attr_gms, p.attr_gms_listing, p.attr_gms_nonseller, p.attr_gms_listing_nonseller, o.attr_gms, o.chargeable_gms
from perf_listings_affiliates p
full outer join osa o using (week, second_channel);
