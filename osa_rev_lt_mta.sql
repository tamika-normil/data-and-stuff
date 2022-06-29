/*

#find osa eligible clicks (for now this is limited to the view listing events ['view_listing', 'view_unavailable_listing', 'view_sold_listing']) for osa eligible channels
#in the future, will update to OSA ads attributed clicks for a more accurate picture

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

#summarize receipt level attribution to record total attribution credit allocated (1) cross channel and (2) last osa eligible channel accredited to OSA ads attributed receipts
CREATE OR REPLACE TEMPORARY table attributed_perf_listings_rank AS (
WITH RECEIPTS AS
#find all receipts associated with OSA eligible visits 
   (SELECT distinct receipt_id
   FROM  feed_visits   AS v
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
feed_visits  ) AS fv on b.o_visit_id = fv.visit_id 
    )
select receipt_id, receipt_timestamp,
#rank visits by visit id or timestamp to estimate last osa eligible visit per receipt 
MAX(CASE WHEN order_channel_rank = 1 and channel_int <> 0 THEN VISIT_ID else null END) AS visit_id,

MAX(CASE WHEN order_channel_rank = 1 and channel_int <> 0 THEN channel_int else null END) AS channel_int,

min(start_datetime) as first_visit_date,
max(gms) as gms,
sum(external_source_decay_all) as receipts,
#channel contribution
#summarize all mta credit assigned to the channel deemed last by OSA adas attributed receipts
 sum(CASE
    WHEN channel_str = concat(reporting_channel_group, ' - ', engine) THEN external_source_decay_all 
    ELSE CAST(NULL as FLOAT64)
    END) as lt_attr_receipt,

#summarize all mta credit assigned to the other channels
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

#summarize all mta GMS assigned to the channel deemed last by OSA ads attributed receipts
#summarize all mta GMS assigned to the other channels

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

#get multi touch channel vs last channel attr receipt and gms
#this is purchase date, reporting_channel_group,engine, level
CREATE OR REPLACE table etsy-data-warehouse-dev.tnormil.charge_lt_mta as
(select date(b.receipt_timestamp) as date,
    v.reporting_channel_group,
    #v.audience, 
    #v.tactic_high_level,
    v.engine, 
    #v.tactic_granular,
    #all touch
    r.channel,
    b.receipt_id, 
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

   max(coalesce(lt_attr_receipt,0)) as lt_attr_receipt_sum,
   max(coalesce(lt_attr_gms,0)) as lt_attr_gms_sum, 

   sum(coalesce(rk.gms,0)) as gms,
   sum(coalesce(rk.receipts,0)) as receipts,

FROM  feed_visits     AS v
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS b ON v.visit_id = b.o_visit_id
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` AS c ON b.receipt_id = c.receipt_id
    LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON b.receipt_id = r.receipt_id
    LEFT OUTER JOIN  attributed_perf_listings_rank rk on b.receipt_id = rk.receipt_id
    group by 1,2,3,4,5,6);
    */

# mta/lt ratio based on total attr receipt per receipt from attribution model [for receipts with OSA rev only]
SELECT date, reporting_channel_group,engine, channel_str, 
sum(attr_receipt_osa) as attr_receipt_osa, 
sum(lt_attr_receipt_sum) as lt_attr_receipt_sum, 
FROM `etsy-data-warehouse-dev.tnormil.charge_lt_mta` 
group by 1,2,3,4;   

# mta/lt based given a credit of 1 for each receipt [for receipts with OSA rev only]
with receipts_mta as
  (select date, reporting_channel_group,engine, count(distinct case when attr_receipt_osa > 0 then receipt_id end) as receipts_mta
  FROM `etsy-data-warehouse-dev.tnormil.charge_lt_mta` 
  group by 1,2,3),
receipts_lt as
  (select date, reporting_channel_group,engine, channel_str, count(distinct case when lt_attr_receipt_sum > 0 then receipt_id end) as receipts_lt
  FROM `etsy-data-warehouse-dev.tnormil.charge_lt_mta` 
  group by 1,2,3,4)
select a.date as purchase_date, a.reporting_channel_group, a.engine,channel_str, receipts_mta, receipts_lt
from receipts_mta a
left join receipts_lt b using (date, reporting_channel_group,engine)
