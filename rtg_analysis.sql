BEGIN


CREATE OR REPLACE TEMPORARY table feed_visits as 
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
        case when v.second_channel in ('gpla', 'intl_gpla') and utm_custom2 in ('14821442085', '14825591657', '14823484966', '14823456235', '14823389971', '14821205487') then 'ssc'
        when v.second_channel in ('gpla', 'intl_gpla') and utm_custom2 not in ('14821442085', '14825591657', '14823484966', '14823456235', '14823389971', '14821205487') then 'non ssc'
        when v.second_channel in ('affiliates') then v.third_channel else null end as tactic,
        reporting_channel_group
        from `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v
        left join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd using (utm_campaign, utm_medium, top_channel, second_channel, third_channel)
        where start_datetime >= '2019-01-01'
        and _date >= '2019-01-01'
        and landing_event in ('view_listing', 'view_unavailable_listing', 'view_sold_listing', 'view_not_available_listing'));

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
    v.tactic,
    count(DISTINCT v.visit_id) AS visits,
    count(DISTINCT CASE WHEN vv.converted = 1 THEN v.visit_id END) AS in_session_converted_visits,
    FROM feed_visits AS v
    left join `etsy-data-warehouse-prod.buyatt_mart.visits_vw` vv on v.visit_id = vv.visit_id and vv.converted = 1
    GROUP BY 1, 2, 3, 4, 5);


 CREATE OR REPLACE TEMPORARY table attributed_perf_listings AS
    (SELECT
    v.date,
    v.listing_id,
    v.marketing_region,
    v.reporting_channel_group,
    v.tactic,
    count(distinct b.receipt_id) as attributed_orders,
    count(distinct b.o_visit_id) as attributed_converting_visits,
    #all touch
    sum(b.external_source_decay_all) AS attr_receipt,
    sum(b.external_source_decay_all * b.gms) AS attr_gms,
    sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,
    sum(CASE
    WHEN r.receipt_id IS NOT NULL THEN b.external_source_decay_all * r.acquisition_fee_usd
    ELSE CAST(NULL as FLOAT64)
    END) AS osa_rev,
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
    FROM feed_visits AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` AS c ON b.receipt_id = c.receipt_id
    LEFT OUTER JOIN attributed_perf_listings_same_shop ss on v.visit_id = ss.visit_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON b.receipt_id = r.receipt_id
      #AND v.channel_int = r.channel
    GROUP BY 1, 2, 3, 4, 5);    

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.attributed_perf_listings` AS 
    ( SELECT
    coalesce(lv.date,att.date) as date,
    coalesce(lv.listing_id, att.listing_id) as listing_id,
    coalesce(lv.marketing_region, att.marketing_region) as marketing_region,
    coalesce(lv.reporting_channel_group, att.reporting_channel_group) as reporting_channel_group,
    coalesce(lv.tactic, att.tactic) as tactic,
    coalesce(sum(lv.visits), 0) as visits,
    coalesce(sum(lv.in_session_converted_visits), 0) as in_session_converted_visits,
    coalesce(sum(att.attributed_orders), 0.0) as attributed_orders,
    coalesce(sum(att.attributed_converting_visits), 0.0) as attributed_converting_visits,
    #all touch
    coalesce(sum(att.attr_receipt), 0.0) AS attr_receipt,
    coalesce(sum(att.attr_gms), 0.0) AS attr_gms,
    coalesce(sum(att.attr_rev), 0.0) AS attr_rev,
    coalesce(sum(att.osa_rev), 0.0) AS osa_rev,
    #same shop
    coalesce(sum(att.attr_receipt_listing), 0.0) AS attr_receipt_listing,
    coalesce(sum(att.attr_gms_listing), 0.0) AS attr_gms_listing,
    coalesce(sum(att.attr_rev_listing), 0.0) AS attr_rev_listing,
    #last click all
    coalesce(sum(att.attr_receipt_last_click_all), 0.0) AS attr_receipt_last_click_all,
    coalesce(sum(att.attr_gms_last_click_all), 0.0) AS attr_gms_last_click_all,
    coalesce(sum(att.attr_rev_last_click_all), 0.0) AS attr_rev_last_click_all,
    #last paid click all
    coalesce(sum(att.attr_receipt_paid_last_click_all), 0.0) AS attr_receipt_paid_last_click_all,
    coalesce(sum(att.attr_gms_paid_last_click_all), 0.0) AS attr_gms_paid_last_click_all,
    coalesce(sum(att.attr_rev_paid_last_click_all), 0.0) AS attr_rev_paid_last_click_all,
    FROM listing_visits AS lv
LEFT OUTER JOIN attributed_perf_listings AS att ON lv.marketing_region = att.marketing_region
    AND lv.tactic = att.tactic
    AND lv.reporting_channel_group = att.reporting_channel_group
    AND lv.listing_id = att.listing_id
    AND lv.date = att.date
    group by 1, 2, 3, 4, 5);
    
#listings with views from gifty campaigns w/o a giftiness score - equates to 30 - 40% of visits    
with listings_w_score as (select distinct listing_id from `etsy-data-warehouse-prod.knowledge_base.listing_giftiness` where _date >= '2021-01-01')
SELECT p.listing_id, sum(visits) as visits, min(date) as first_click_date, max(date) as last_click_date
FROM `etsy-data-warehouse-dev.tnormil.attributed_perf_listings` p
left join listings_w_score lg on p.listing_id = lg.listing_id
where tactic = 'ssc'
and date > '2021-11-11'
and lg.listing_id is null
group by 1
order by visits desc;

END;
