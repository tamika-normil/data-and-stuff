#process last 10 days of purchases to rebuild daily oci data

BEGIN 

-- getting all the receipts from the past day that are GDPR compliant so as to prevent sending data for users who have opted out
create temporary table gdpr_check as (
   select (select cast(value as INT64) from unnest(properties.map) where key = "receipt_id") AS receipt_id,
   from `etsy-visit-pipe-prod.canonical.beacons_recent`
   where
   #updated to 10 days
       date(_PARTITIONTIME) >= (current_date-10) and date(_PARTITIONTIME) < (current_date)
       and event_name = 'cart_payment'
       and (select cast(value as INT64) from unnest(properties.map) where key = "gdpr_tp") not in (0, 2)
);


-- summing prolist revenue to the visit level
create temporary table prolist_visits as (
    select visit_id, sum(cost)/100 as prolist_revenue
    from `etsy-data-warehouse-prod.ads.prolist_click_visits`
    where _date>=(current_date-31) and run_date>=unix_seconds(timestamp(current_date-31))
    group by 1
);


create or replace table `etsy-data-warehouse-dev.buyatt_mart.daily_oci_data` as 
with base as (
    select a.receipt_id, 
        case when landing_event_url like '%gclid%' 
            then split(split(landing_event_url,'gclid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
            when landing_event_url like '%gbraid%' 
            then split(split(landing_event_url,'gbraid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
            when landing_event_url like '%wbraid%' 
            then split(split(landing_event_url,'wbraid=')[safe_ordinal(2)],'&')[safe_ordinal(1)]
        end as click_id,
        case when landing_event_url like '%gclid%'
           then 'gclid'
           when landing_event_url like '%gbraid%'
           then 'gbraid'
           when landing_event_url like '%wbraid%'
           then 'wbraid'
       end as click_type,
       top_channel,
        b.browser_id,
        sum(a.external_source_decay_all) as attribution_fraction,
        coalesce(sum(prolist_revenue),0) as prolist_revenue
    from `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` a 
    join gdpr_check g on a.receipt_id=g.receipt_id
    join `etsy-data-warehouse-prod.buyatt_mart.visits` b on a.o_visit_run_date = b.run_date
        and a.o_visit_id = b.visit_id
    left join prolist_visits c on b.visit_id=c.visit_id
    #updated to 10 days
    where date(receipt_timestamp)>= (current_date-10) 
        and utm_source='google' and utm_medium in ('cpc') and utm_campaign not like 'gdn_%' and utm_campaign not like 'gda_%'
        and top_channel in ('us_paid','intl_paid')
        and b._date>=(current_date-31) and b.run_date>=unix_seconds(timestamp(current_date-31))
    group by 1,2,3,4,5
)
select 
    purchase_date,
    a.receipt_id, 
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
using (receipt_id);

END;

#last 10 days of volume by click type
SELECT purchase_date, click_type, count(*)
FROM  etsy-data-warehouse-dev.buyatt_mart.daily_oci_data 
WHERE purchase_date >= "2022-06-01" 
group by 1, 2;
