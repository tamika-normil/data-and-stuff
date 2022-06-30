BEGIN 

/*
CREATE OR REPLACE TEMPORARY table feed_visits as (with 
base as 
        (select v.visit_id,
        v.marketing_region,
        date(start_datetime) as date, coalesce(CAST(regexp_extract(landing_event_url, '(\?i)listing\\/(\\d{1,9})', 1, 1) as INT64), -1) AS listing_id,
        CASE
            WHEN second_channel IN(
            'gpla', 'intl_gpla'
            ) THEN 1
            WHEN second_channel IN(
            'facebook_disp', 'facebook_disp_intl'
            ) THEN 2
            WHEN second_channel = 'instagram_disp' THEN 3
            WHEN second_channel IN(
            'bing_plas', 'intl_bing_plas'
            ) THEN 4
            WHEN second_channel = 'pinterest_disp' THEN 5
            WHEN second_channel = 'affiliates' and (third_channel = 'affiliates_feed' or third_channel = 'affiliates_widget' ) THEN 6		
            WHEN lower(utm_campaign) like 'gdn_%' then 7
            ELSE 0
        END AS channel_int,
        reporting_channel_group,
        audience,
        engine, 
        tactic_high_level,
        tactic_granular,
        case when v.second_channel in ('affiliates') then case when lower(utm_term) like '%-%' then regexp_replace(SPLIT(utm_term,'-')[OFFSET(1)], r'[0-9](?i)feed(?i)$|(?i)feed(?i)$', '') else utm_term end end as feed,
        converted,
        case when v.second_channel in ('affiliates') then left(utm_content,15) end as utm_content,
        from `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v
        left join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd using (utm_campaign, utm_medium, top_channel, second_channel, third_channel)
        where start_datetime >= '2021-07-01'
        and _date >= '2021-07-01'
        and landing_event in ('view_listing', 'view_unavailable_listing', 'view_sold_listing')
        and (tactic_high_level = "Affiliates - Feed" or reporting_channel_group in ('PLA', 'Display', 'Paid Social')))
 select *,
    case when regexp_contains(feed,r'[^a-zA-Z]') then 'contains invalid char'
   when feed is null then 'utm term empty'
   else lower(feed) end as feed_fix,
   case when regexp_contains(feed,r'[^a-zA-Z]') then 'contains invalid char'
   when feed is null then 'utm term empty'
   when regexp_contains(lower(feed), r"^accessories|^art|^bath|^clothing|^craft|^gifts|^home|^jewelry|^wedding|^book|^toy")
   then 'category intent'
   else lower(feed) end as feed_type,
 from base);       

CREATE OR REPLACE TEMPORARY table prolist_revenue AS
    (
    select visit_id, sum(cost/100) as cost
    from `etsy-data-warehouse-prod.ads.prolist_click_visits` p 
    INNER JOIN feed_visits r using (visit_id)
    where p._date >= '2021-07-01'
    group by 1
    );    

CREATE OR REPLACE TEMPORARY table attributed_perf_listings_same_shop AS
    (SELECT
    v.visit_id,
    max(CASE WHEN `at`.receipt_id IS NOT NULL then 1 else 0 end) as same_shop_ind
    FROM feed_visits  AS v
    INNER JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` AS l ON v.listing_id = l.listing_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` AS `at` ON b.receipt_id = `at`.receipt_id AND l.user_id = `at`.seller_user_id
    GROUP BY 1);  



CREATE OR REPLACE TEMPORARY TABLE listing_visits as
    (SELECT
    v.date,
    v.listing_id,
    v.marketing_region,
   v.reporting_channel_group,
   v.audience, 
   v.tactic_high_level,
   v.feed_fix,
   v.feed_type,
   v.utm_content,
   v.engine, 
   v.tactic_granular,
    count(DISTINCT v.visit_id) AS visits,
    count(DISTINCT CASE WHEN v.converted = 1 THEN v.visit_id END) AS in_session_converted_visits,
  sum(coalesce(cost,0)) as prolist_revenue
    FROM etsy-bigquery-adhoc-prod._script98630ffc0a95ed547af377d7384e56976f23289d.feed_visits  AS v
   left join  etsy-bigquery-adhoc-prod._script98630ffc0a95ed547af377d7384e56976f23289d.prolist_revenue  as p on v.visit_id = p.visit_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);    


CREATE OR REPLACE TEMPORARY table attributed_perf_listings_rank AS (
WITH RECEIPTS AS
   (SELECT distinct receipt_id
   FROM etsy-bigquery-adhoc-prod._script98630ffc0a95ed547af377d7384e56976f23289d.feed_visits AS v
   INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id),
RNK_FEED_VISIT AS  
    (SELECT
    v.visit_id,
    v.start_datetime,
    b.receipt_id,
    b.receipt_timestamp,
    fv.channel_int,
    fv.osa,
    b.external_source_decay_all,
    b.gms,
    b.buyer_type,
    reporting_channel_group, 
    row_number() OVER (PARTITION BY b.receipt_id,osa ORDER BY fv.visit_id DESC) AS order_channel_rank,
    FROM RECEIPTS r
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON r.receipt_id = b.receipt_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v on b.o_visit_id = v.visit_id
    left outer join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd using (utm_campaign, utm_medium, top_channel, second_channel, third_channel)
    left outer join (select visit_id, channel_int, case when channel_int <> 0 then 1 else 0 end as osa from etsy-bigquery-adhoc-prod._script98630ffc0a95ed547af377d7384e56976f23289d.feed_visits) AS fv on b.o_visit_id = fv.visit_id)
select receipt_id, receipt_timestamp,
MAX(CASE WHEN order_channel_rank = 1 and channel_int <> 0 THEN VISIT_ID else null END) AS visit_id,
MAX(CASE WHEN order_channel_rank = 1 and channel_int <> 0 THEN channel_int else null END) AS channel_int,

min(start_datetime) as first_visit_date,
max(gms) as gms,
sum(external_source_decay_all) as receipts,

#channel contribution
 sum(CASE
    WHEN reporting_channel_group = 'PLA' THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as plas_attr_receipt,
 sum(CASE
    WHEN reporting_channel_group = 'Direct' THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as direct_attr_receipt,
 sum(CASE
    WHEN reporting_channel_group in ('Email','Push') THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as crm_attr_receipt,
 sum(CASE
    WHEN reporting_channel_group = 'Paid Social' THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as paid_social_attr_receipt,  
 sum(CASE
    WHEN reporting_channel_group = 'Display' THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as display_attr_receipt,

 sum(CASE
    WHEN reporting_channel_group = 'PLA' THEN external_source_decay_all * gms
    ELSE CAST(NULL as FLOAT64)
    END) as plas_attr_gms,
 sum(CASE
    WHEN reporting_channel_group = 'Direct' THEN external_source_decay_all * gms
    ELSE CAST(NULL as FLOAT64)
    END) as direct_attr_gms,
 sum(CASE
    WHEN reporting_channel_group in ('Email','Push') THEN external_source_decay_all * gms
    ELSE CAST(NULL as FLOAT64)
    END) as crm_attr_gms,
 sum(CASE
    WHEN reporting_channel_group = 'Paid Social' THEN external_source_decay_all * gms
    ELSE CAST(NULL as FLOAT64)
    END) as paid_social_attr_gms,  
 sum(CASE
    WHEN reporting_channel_group = 'Display' THEN external_source_decay_all * gms
    ELSE CAST(NULL as FLOAT64)
    END) as display_attr_gms,      
 from RNK_FEED_VISIT 
GROUP BY 1,2);

CREATE OR REPLACE table etsy-data-warehouse-dev.tnormil.attributed_perf_listings_sample_af2 AS
    (SELECT
    v.date,
    v.listing_id,
    v.marketing_region,
    v.reporting_channel_group,
    v.audience, 
    v.tactic_high_level,
    v.feed_fix,
    v.feed_type,
    v.utm_content,
    count(distinct b.receipt_id) as attributed_orders,
    count(distinct b.o_visit_id) as attributed_converting_visits,
    #all touch
    sum(b.external_source_decay_all) AS attr_receipt,
    sum(b.external_source_decay_all * b.gms) AS attr_gms,
    sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,
    sum(receipts) as total_receipts,
    #same shop
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
    END) AS attr_rev_listing,
    #chargeable
    SUM(CASE
    WHEN r.receipt_id IS NOT NULL
    AND rk.VISIT_ID is not null THEN b.gms 
    ELSE CAST(NULL as FLOAT64)
    END) AS gms_chargeable,
    count(distinct CASE
    WHEN r.receipt_id IS NOT NULL
    AND rk.VISIT_ID is not null THEN b.receipt_id
    ELSE CAST(NULL as FLOAT64)
    END) AS receipts_chargeable,
    SUM(acquisition_fee_usd) AS osa_rev,
#buyer type

   sum(attributed_new_receipts) as attributed_new_receipts,
   sum(attributed_lapsed_receipts) as attributed_lapsed_receipts,

#channel contribution
   sum(plas_attr_receipt) as plas_attr_receipt,
   sum(direct_attr_receipt) as direct_attr_receipt,
   sum(crm_attr_receipt) as crm_attr_receipt,
   sum(paid_social_attr_receipt) as paid_social_attr_receipt, 
   sum(display_attr_receipt) as display_attr_receipt, 

    FROM  feed_visits  AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` AS c ON b.receipt_id = c.receipt_id
    LEFT OUTER JOIN  attributed_perf_listings_same_shop ss on v.visit_id = ss.visit_id
    LEFT OUTER JOIN attributed_perf_listings_rank rk on v.visit_id = rk.visit_id and b.receipt_id = rk.receipt_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON b.receipt_id = r.receipt_id
      AND rk.channel_int = r.channel
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9); 
     


CREATE OR REPLACE table etsy-data-warehouse-dev.tnormil.attributed_perf_listings_sample_af AS
    (SELECT
    v.date,
    v.listing_id,
    v.marketing_region,
    v.reporting_channel_group,
    v.audience, 
    v.tactic_high_level,
    v.feed_fix,
    v.feed_type,
    v.utm_content,
            v.engine, 
        v.tactic_granular,
    count(distinct b.receipt_id) as attributed_orders,
    count(distinct b.o_visit_id) as attributed_converting_visits,
    #all touch
    sum(b.external_source_decay_all) AS attr_receipt,
    sum(b.external_source_decay_all * b.gms) AS attr_gms,
    sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,
    #last click all
    sum(last_click_all) AS attr_receipt_last_click_all,
    sum(last_click_all * b.gms) AS attr_gms_last_click_all,
    sum(last_click_all * c.attr_rev) AS attr_rev_last_click_all,
    #last paid click all
    sum(paid_last_click_all) AS attr_receipt_paid_last_click_all,
    sum(paid_last_click_all * b.gms) AS attr_gms_paid_last_click_all,
    sum(paid_last_click_all* c.attr_rev) AS attr_rev_paid_last_click_all,
    #same shop
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
    END) AS attr_rev_listing,
    #chargeable
    max(CASE
    WHEN r.receipt_id IS NOT NULL
    AND rk.VISIT_ID is not null THEN b.gms 
    ELSE CAST(NULL as FLOAT64)
    END) AS gms_chargeable,
    count(distinct CASE
    WHEN r.receipt_id IS NOT NULL
    AND rk.VISIT_ID is not null THEN b.receipt_id
    ELSE CAST(NULL as FLOAT64)
    END) AS receipts_chargeable,
    sum(external_source_decay_all*r.acquisition_fee_usd/100) as attr_osa_rev,
    #buyers type,
   sum(external_source_decay_all*(cast(b.buyer_type= 'new' as int64))) as attributed_new_receipts,
   sum(external_source_decay_all*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_receipts,
   sum(external_source_decay_all*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_receipts,

   sum(external_source_decay_all*b.gms*(cast(b.buyer_type= 'new' as int64))) as attributed_new_gms,
   sum(external_source_decay_all*b.gms*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_gms,
   sum(external_source_decay_all*b.gms*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_gms,
    #mta dim
    avg(date_diff(b.receipt_timestamp,first_visit_date, DAY)) as days_between_visit_purchase,
    stddev(date_diff(b.receipt_timestamp,first_visit_date, DAY)) as days_between_visit_purchase_stddev, 

#channel contribution
   avg(safe_divide(plas_attr_receipt, receipts)) as plas_attr_receipt,
   avg(safe_divide(direct_attr_receipt, receipts)) as direct_attr_receipt,
   avg(safe_divide(crm_attr_receipt, receipts)) as crm_attr_receipt,
   avg(safe_divide(paid_social_attr_receipt, receipts)) as paid_social_attr_receipt, 
   avg(safe_divide(display_attr_receipt, receipts)) as display_attr_receipt, 

   stddev(safe_divide(plas_attr_receipt, receipts)) as plas_attr_receipt_stddev,
   stddev(safe_divide(direct_attr_receipt, receipts)) as direct_attr_receipt_stddev,
   stddev(safe_divide(crm_attr_receipt, receipts)) as crm_attr_receipt_stddev,
   stddev(safe_divide(paid_social_attr_receipt, receipts)) as paid_social_attr_receipt_stddev, 
   stddev(safe_divide(display_attr_receipt, receipts)) as display_attr_receipt_stddev, 

    FROM  etsy-bigquery-adhoc-prod._script98630ffc0a95ed547af377d7384e56976f23289d.feed_visits   AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` AS c ON b.receipt_id = c.receipt_id
    LEFT OUTER JOIN  etsy-bigquery-adhoc-prod._script98630ffc0a95ed547af377d7384e56976f23289d.attributed_perf_listings_same_shop  ss on v.visit_id = ss.visit_id
    LEFT OUTER JOIN etsy-bigquery-adhoc-prod._scriptd6eda604948fa6c536b4244ba305522e0a3bcfad.attributed_perf_listings_rank rk on v.visit_id = rk.visit_id and b.receipt_id = rk.receipt_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON b.receipt_id = r.receipt_id
      AND rk.channel_int = r.channel
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 , 10, 11);

 */      

CREATE OR REPLACE TEMPORARY TABLE static_perf_listing  AS 
((with shipping_profiles as
    (SELECT shipping_profile_id FROM `etsy-data-warehouse-prod.etsy_shard.shipping_profile`
    where (min_processing_days <= 1 OR max_processing_days <= 2 OR min_processing_days is null)),
listing_shipping_profiles as
    (SELECT listing_id
    from `etsy-data-warehouse-prod.etsy_shard.listing_shipping_profile` 
    where shipping_profile_id in (select * from shipping_profiles))
select distinct 
l.listing_id, 
l.shop_id,
case when top_category is null then 'other' else top_category end as top_category , 
case when second_level_cat_new is null then 'other' else second_level_cat_new end as subcategory, 
cast(timestamp_seconds(l.original_create_date) as date) as original_create_date,
open_date as shop_open_date, 
case when pt.category is not null and l.price/100 < pt.e then 'e'
when pt.category is not null and l.price/100 < pt.d and l.price >= pt.e then 'd'
when pt.category is not null and l.price/100 < pt.c and l.price >= pt.d then 'c'
when pt.category is not null and l.price/100 < pt.b and l.price >= pt.c then 'b'
when pt.category is not null and l.price/100 >= pt.a then 'a'
when pt.category is null and l.price/100 < 6.50 then 'e'
when pt.category is null and l.price/100 < 13 and l.price >= 6.50 then 'd'
when pt.category is null and l.price/100 < 22 and l.price >= 13 then 'c' 
when pt.category is null and l.price/100 < 40 and l.price >= 22 then 'b' 
when pt.category is null and l.price/100 >= 40 then 'a' end as price_tier,
L.price/100 as price,
case when sp.listing_id is not null then 1 else 0 end as  rts,
case when l.color is not null then 1 else 0 end as has_color,
score as nsfw_score,
is_download,
olf.category,
olf.is_bestseller,
olf.seller_tier,
olf.seller_tier_gpla,
case when i.iso_country_code in ('AU', 'CA', 'DE', 'ES', 'FR', 'IE', 'IT', 'NL','US', 'GB') then i.iso_country_code else 'RoW' end as seller_country, 
from `etsy-data-warehouse-prod.listing_mart.listing_vw` l
left join `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` c on l.listing_id = c.listing_id
left join `etsy-data-warehouse-prod.rollups.seller_basics` s on l.shop_id = s.shop_id
left join `etsy-data-warehouse-prod.static.msts_countries` i on s.country_name = i.country
left join `etsy-data-warehouse-prod.static.price_tier` pt on l.top_category = pt.category
left join  `etsy-sr-etl-prod.nsfw_score.inferenced_all_listings` nsfw on l.listing_id = nsfw.listing_id
left join listing_shipping_profiles sp on l.listing_id = sp.listing_id 
left join `etsy-data-warehouse-prod.olf.olf_hydration_daily` olf on l.listing_id = olf.listing_id 
and DATE(olf._PARTITIONTIME) = current_date - 1));

/*
CREATE OR REPLACE TABLE etsy-data-warehouse-dev.tnormil.perf_listings_sample_af  AS 
WITH BASE AS
    ( SELECT
    lv.date,
    lv.listing_id,
    lv.marketing_region,
    lv.reporting_channel_group,
    lv.audience, 
    lv.tactic_high_level,
    lv.feed_fix,
    lv.feed_type,	
    lv.utm_content, 
    coalesce(sum(lv.visits), 0) as visits,
    coalesce(sum(lv.prolist_revenue), 0) as prolist_revenue, 
    coalesce(sum(lv.in_session_converted_visits), 0) as in_session_converted_visits,
    #all touch
    sum(attr_receipt) as attr_receipt,
    sum(attr_gms) as attr_gms,
     (coalesce(sum(att.attr_rev), 0.0) + coalesce(sum(lv.prolist_revenue),0)) AS attr_rev,
    sum(total_receipts) as total_receipts,
    #same shop
    sum(attr_receipt_listing) as attr_receipt_listing,
    sum(attr_gms_listing) as attr_gms_listing,
    sum(attr_rev_listing) as attr_rev_listing,
    #chargeable
    SUM(gms_chargeable) as gms_chargeable,
    SUM(receipts_chargeable) as receipts_chargeable,
    SUM(attr_osa_rev) as attr_osa_rev,
#buyer type

   SUM(attributed_new_receipts) AS attributed_new_receipts,
   sum(attributed_lapsed_receipts) as attributed_lapsed_receipts,
   sum(attributed_existing_receipts) as attributed_existing_receipts,
   sum(attributed_new_gms) as attributed_new_gms,
   sum(attributed_lapsed_gms) as attributed_lapsed_gms,
   sum(attributed_existing_gms) as attributed_existing_gms,

#channel contribution
   sum(plas_attr_receipt) as plas_attr_receipt,
   sum(direct_attr_receipt) as direct_attr_receipt,
   sum(crm_attr_receipt) as crm_attr_receipt,
   sum(paid_social_attr_receipt) as paid_social_attr_receipt, 
   sum(display_attr_receipt) as display_attr_receipt, 

    FROM listing_visits AS lv
    LEFT OUTER JOIN etsy-data-warehouse-dev.tnormil.attributed_perf_listings_sample_af AS att on lv.date = att.date
    AND lv.listing_id = att.listing_id
    AND lv.marketing_region = att.marketing_region
    AND lv.reporting_channel_group = att.reporting_channel_group
    AND lv.audience = att.audience
    AND lv.tactic_high_level = att.tactic_high_level
    AND lv.feed_fix = att.feed_fix
    AND lv.feed_type = att.feed_type	
    AND lv.utm_content = att.utm_content
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9)
SELECT *
FROM BASE 
LEFT JOIN static_perf_listing USING (LISTING_ID);
*/

CREATE OR REPLACE TABLE etsy-data-warehouse-dev.tnormil.perf_listings_sample_af  AS 
WITH BASE AS
    ( SELECT lv.*,
    #all touch
    attr_receipt,
    attr_gms,
    attr_rev,
    #same shop
    attr_receipt_listing,
    attr_gms_listing,
    attr_rev_listing,
    #chargeable
    gms_chargeable,
    receipts_chargeable,
    attr_osa_rev,
    #buyer type
    attributed_new_receipts,
    attributed_lapsed_receipts,
    attributed_existing_receipts,
    attributed_new_gms,
    attributed_lapsed_gms,
    attributed_existing_gms,
   #channel contribution
   plas_attr_receipt,
   direct_attr_receipt,
   crm_attr_receipt,
   paid_social_attr_receipt, 
   display_attr_receipt, 
    FROM etsy-bigquery-adhoc-prod._scriptd6eda604948fa6c536b4244ba305522e0a3bcfad.listing_visits AS lv
    LEFT OUTER JOIN etsy-data-warehouse-dev.tnormil.attributed_perf_listings_sample_af AS att on lv.date  = att.date
    AND lv.listing_id = att.listing_id
    AND coalesce(lv.marketing_region,'') = coalesce(att.marketing_region,'')
    AND coalesce(lv.reporting_channel_group,'') = coalesce(att.reporting_channel_group,'')
    AND coalesce(lv.audience,'') = coalesce(att.audience,'')
    AND coalesce(lv.tactic_high_level,'') = coalesce(att.tactic_high_level,'')
    AND coalesce(lv.feed_fix,'') = coalesce(att.feed_fix,'')
    AND coalesce(lv.feed_type,'') = coalesce(att.feed_type,'')	
    AND coalesce(lv.utm_content,'') = coalesce(att.utm_content,'')
    AND coalesce(lv.engine,'') = coalesce(lv.engine,'')
    AND coalesce(lv.tactic_high_level,'') = coalesce(att.tactic_high_level,'')
    AND coalesce(lv.tactic_granular,'') = coalesce(att.tactic_granular,''))
SELECT *
FROM BASE 
LEFT JOIN static_perf_listing USING (listing_id);

END

/*
COUNT(channel_high_level) as all_channel_count,
COUNT(distinct channel_high_level) as distinct_channel_count,
count(CASE WHEN channel_int <> 0 THEN channel_high_level END) as all_osa_channel_count,
count(distinct CASE WHEN channel_int <> 0 THEN channel_int END) as distinct_osa_channel_count,
*/
