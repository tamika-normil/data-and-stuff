#process last 10 days of purchases to rebuild daily oci data
#update att opt in, prolist window to 41 days (from 31 days)

/*
ROLLUP FOR GOOGLE OFFLINE CONVERSION IMPORT
Documentation: https://docs.google.com/document/d/1yIxy_idLdTCq4d3AGramW3TCr_qXABRi4x7KpIE0zyk/edit#
owner: vbhuta@etsy.com
owner team: marketinganalytics@etsy.com
*/

BEGIN 

-- getting all the receipts from the past day that are GDPR compliant so as to prevent sending data for users who have opted out
create temporary table gdpr_check as (
with base as (
    select mapped_user_id, 
        max(create_date) as latest
    from `etsy-data-warehouse-prod.user_mart.user_mapping`
    group by 1
)
,latest_user as (
    select 
        a.mapped_user_id, 
        user_id
    from `etsy-data-warehouse-prod.user_mart.user_mapping` a
    join base b on a.mapped_user_id=b.mapped_user_id and a.create_date=b.latest
)
, privacy_details as (
    select user_id 
    from `etsy-data-warehouse-prod.etsy_shard.user_privacy_details`
    where third_party_integration_allowed=1
    union distinct
    select guest_user_id 
    from `etsy-data-warehouse-prod.etsy_shard.guest_user_privacy_details`
    where third_party_integration_allowed=1
)
select 
    distinct mapped_user_id, receipt_id
from latest_user a 
join privacy_details b using (user_id)
join `etsy-data-warehouse-prod.transaction_mart.all_receipts` c using (mapped_user_id)
#updated to 10 days
where date(creation_tsz)>=(current_date-10) 
);

-- getting visits from ATT opted-in users. only opted-in users will have the ios_advertising_id
create temporary table att_visits as (
    select distinct visit_id
    from `etsy-data-warehouse-prod.etsy_aux.appsflyer` a 
    join `etsy-data-warehouse-prod.buyer_growth.native_ids` b on a.ios_advertising_id=b.idfa
    and a.att_status=1 and b.event_source='ios'
    #updated to 41 days
    where b._date>=(current_date-41)
);

-- summing prolist revenue to the visit level
create temporary table prolist_visits as (
    select visit_id, sum(cost)/100 as prolist_revenue
    from `etsy-data-warehouse-prod.ads.prolist_click_visits`
    #updated to 41 days
    where _date>=(current_date-41) and run_date>=unix_seconds(timestamp(current_date-41))
    group by 1
);

drop table `etsy-data-warehouse-dev.buyatt_mart.daily_oci_data`;


create table `etsy-data-warehouse-dev.buyatt_mart.daily_oci_data` 
(receipt_id int64,
receipt_timestamp TIMESTAMP,
click_id STRING,
browser_id STRING,
click_type STRING,
top_channel STRING,
base_revenue float64,
base_attr_fraction float64,
base_attr_model STRING,
osa_revenue float64,
osa_attr_fraction float64,
osa_attr_model STRING,
attributed_ltv_revenue float64,
ltv_attr_fraction float64,
ltv_attr_model STRING,
commission_revenue float64,
commission_attr_fraction float64,
commission_attr_model STRING,
prolist_revenue NUMERIC,
prolist_attr_fraction int64,
prolist_attr_model STRING)  
partition by datetime_trunc(receipt_timestamp, hour) as
with base as (
    select a.receipt_id, 
        receipt_timestamp,
         -- for BOE iOS, need to send gbraid/wbraid for ATT opted out users
        case when mapped_platform_type like 'boe_ios%' and d.visit_id is null then 
            case when landing_event_url like '%gbraid%' 
                then split(split(landing_event_url,'gbraid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
                when landing_event_url like '%wbraid%' 
                then split(split(landing_event_url,'wbraid=')[safe_ordinal(2)],'&')[safe_ordinal(1)] 
            end
          else 
            case when landing_event_url like '%gclid%' 
                then split(split(landing_event_url,'gclid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
                when landing_event_url like '%gbraid%' 
                then split(split(landing_event_url,'gbraid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
                when landing_event_url like '%wbraid%' 
                then split(split(landing_event_url,'wbraid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
            end
        end as click_id,
        case when mapped_platform_type like 'boe_ios%' and d.visit_id is null then 
            case when landing_event_url like '%gbraid%'
                then 'gbraid'
                when landing_event_url like '%wbraid%'
                then 'wbraid'
            end
          else 
            case when landing_event_url like '%gclid%'
                then 'gclid'
                when landing_event_url like '%gbraid%'
                then 'gbraid'
                when landing_event_url like '%wbraid%'
                then 'wbraid'
            end
        end as click_type,
       top_channel,
        b.browser_id,
        sum(a.external_source_decay_all) as attribution_fraction,
        coalesce(sum(prolist_revenue),0) as prolist_revenue
    from `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` a 
    join gdpr_check g on a.receipt_id=g.receipt_id
    join `etsy-data-warehouse-prod.buyatt_mart.visits` b on a.o_visit_run_date = b.run_date
        and a.o_visit_id = b.visit_id
    left join att_visits d using (visit_id)
    left join prolist_visits c on b.visit_id=c.visit_id
    #updated to 10 days
    where date(receipt_timestamp)>= (current_date-10) 
        and utm_source='google' and utm_medium in ('cpc') and utm_campaign not like 'gdn_%' and utm_campaign not like 'gda_%'
        and top_channel in ('us_paid','intl_paid')
        #updated to 41 days
        and b._date>=(current_date-41) and b.run_date>=unix_seconds(timestamp(current_date-41))
    group by 1,2,3,4,5,6
)
select 
    a.receipt_id, 
    receipt_timestamp,
    click_id,
    browser_id,
    click_type,
    top_channel,
    
    attr_rev*attribution_fraction as base_revenue, -- attributed revenue (w/o prolist), this is similar to what is currently passed in pixel 
    attribution_fraction as base_attr_fraction,
    'time_decay_mta' as base_attr_model,

    etsy_ads_revenue*attribution_fraction as osa_revenue, -- osa revenue attributed per MTA model
    attribution_fraction as osa_attr_fraction,
    'time_decay_mta' as osa_attr_model,

    case when ((ltv_gms-round(day_gms*day_percent,2))*.112)*attribution_fraction <0 
        then 0 else ((ltv_gms-round(day_gms*day_percent,2))*.112)*attribution_fraction 
    end as attributed_ltv_revenue,
    attribution_fraction as ltv_attr_fraction, -- ltv revenue attributed per MTA model
    'time_decay_mta' as ltv_attr_model,

    (receipt_gms*.112)*attribution_fraction as commission_revenue, -- using 11.2% average take rate
    attribution_fraction as commission_attr_fraction,
    'time_decay_mta' as commission_attr_model,

    prolist_revenue, -- prolist revenue is not attributed. if a click generates osa revenue, it's attributed to that click
    1 as prolist_attr_fraction,
    'unattributed' as prolist_attr_model
from base a 
join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` b 
using (receipt_id)
;


end;

SELECT date(receipt_timestamp) as timestamp, click_type, count(*) as receipts, sum(base_revenue) as base_revenue
FROM  etsy-data-warehouse-dev.buyatt_mart.daily_oci_data 
WHERE  date(receipt_timestamp)  >= "2022-06-01" 
group by 1, 2
order by 1,2;
