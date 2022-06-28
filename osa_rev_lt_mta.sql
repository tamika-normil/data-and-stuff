#find visits to listings 
#in the future, will update to ads attributed clicks 
CREATE OR REPLACE TEMPORARY table feed_visits as 
        (select v.visit_id,
        v.marketing_region,
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
        date(start_datetime) as date
        from `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v
        left join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd using (utm_campaign, utm_medium, top_channel, second_channel, third_channel)
        where start_datetime >= '2022-01-01'
        and _date >= '2022-01-01'
        and landing_event in ('view_listing', 'view_unavailable_listing', 'view_sold_listing')
        and (tactic_high_level = "Affiliates - Feed" or reporting_channel_group in ('PLA', 'Display', 'Paid Social')));

#summarize receipt level attribution
#get last channel clicked for receipt and total attribution by that channel
CREATE OR REPLACE TEMPORARY table attributed_perf_listings_rank AS (
WITH RECEIPTS AS
   (SELECT distinct receipt_id
   FROM  etsy-bigquery-adhoc-prod._script54c90cb81f618fde36e02a15bacbdd386536f52a.feed_visits   AS v
   INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id),
RNK_FEED_VISIT AS  
    (SELECT
    v.visit_id,
    v.start_datetime,
    b.receipt_id,
    b.receipt_timestamp,
    channel_int, 
    osa,
    r2.channel,
    case when r2.channel = 1 then 'PLA - Google - Paid'
    when r2.channel in (2,3) then 'Paid Social - Facebook - Paid'
    when r2.channel = 4 then 'PLA - Bing - Paid'
    when r2.channel = 5 then 'Paid Social - Pinterest - Paid'
    when r2.channel = 6 then 'Affiliates - Affiliates'
    when r2.channel = 7 then 'Display - Google - Paid' end as channel_str,
    reporting_channel_group, 
    engine,
    b.external_source_decay_all,
    b.gms,
    b.buyer_type,
    row_number() OVER (PARTITION BY b.receipt_id,osa ORDER BY fv.visit_id DESC) AS order_channel_rank,
    FROM RECEIPTS r
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON r.receipt_id = b.receipt_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits_vw` v on b.o_visit_id = v.visit_id
    left outer join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd using (utm_campaign, utm_medium, top_channel, second_channel, third_channel)
    left outer join `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r2 ON b.receipt_id = r2.receipt_id   
    left outer join (select visit_id, channel_int, case when channel_int <> 0 then 1 else 0 end as osa from 
 etsy-bigquery-adhoc-prod._script54c90cb81f618fde36e02a15bacbdd386536f52a.feed_visits  ) AS fv on b.o_visit_id = fv.visit_id 
    )
select receipt_id, receipt_timestamp,
MAX(CASE WHEN order_channel_rank = 1 and channel_int <> 0 THEN VISIT_ID else null END) AS visit_id,
MAX(CASE WHEN order_channel_rank = 1 and channel_int <> 0 THEN channel_int else null END) AS channel_int,

min(start_datetime) as first_visit_date,
max(gms) as gms,
sum(external_source_decay_all) as receipts,

#channel contribution
 sum(CASE
    WHEN channel_str = concat(reporting_channel_group, ' - ', engine) THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as lt_attr_receipt,
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
    WHEN channel_str = concat(reporting_channel_group, ' - ', engine) THEN external_source_decay_all * gms
    ELSE CAST(NULL as FLOAT64)
    END) as lt_attr_gms,
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

#summarize receipt level attribution
#get multi touch channel vs last channel attr receipt and gms
#this is purchcase date channel level
select date(b.receipt_timestamp) as date,
    v.reporting_channel_group,
    #v.audience, 
    #v.tactic_high_level,
    v.engine, 
    #v.tactic_granular,
    #all touch
    r.channel,
    case when r.channel = 1 then 'PLA - Google - Paid'
    when r.channel in (2,3) then 'Paid Social - Facebook - Paid'
    when r.channel = 4 then 'PLA - Bing - Paid'
    when r.channel = 5 then 'Paid Social - Pinterest - Paid'
    when r.channel = 6 then 'Affiliates - Affiliates'
    when r.channel = 7 then 'Display - Google - Paid' end as channel_str,
    sum(b.external_source_decay_all) AS attr_receipt,
    sum(case when r.acquisition_fee_usd is not null then b.external_source_decay_all end) AS attr_receipt_osa,
    sum(b.external_source_decay_all * b.gms) AS attr_gms,
    sum(case when r.acquisition_fee_usd is not null then b.external_source_decay_all * b.gms end) AS attr_gms_osa,   
    sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,

   sum(coalesce(lt_attr_receipt,0)) as lt_attr_receipt_sum,
   sum(coalesce(lt_attr_gms,0)) as lt_attr_gms_sum, 

   sum(coalesce(rk.gms,0)) as gms,
   sum(coalesce(rk.receipts,0)) as receipts,

FROM  etsy-bigquery-adhoc-prod._script194981d6eabe97a1ba43f2a21fe1d7133dc557fc.feed_visits     AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` AS c ON b.receipt_id = c.receipt_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON b.receipt_id = r.receipt_id
    LEFT OUTER JOIN  etsy-bigquery-adhoc-prod._script1f2880834fbb49814cc263ba34e2a2bfe7d03a6b.attributed_perf_listings_rank rk on b.receipt_id = rk.receipt_id
    group by 1,2,3,4,5
