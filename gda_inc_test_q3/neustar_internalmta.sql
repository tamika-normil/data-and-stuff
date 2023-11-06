with daily_tracker as 
(select date(date_trunc(day, month)) as month,reporting_channel_group, 
case when lower(account_name) like '% us%' then 'US'
            when lower(account_name) like '% uk%' then 'GB'
            when lower(account_name) like '% ca%' then 'CA'
            when lower(account_name) like '% fr%' then 'FR'
            when lower(account_name) like '% au%' then 'AU'
            when lower(account_name) like '% de%' then 'DE'
            when lower(account_name) like '% ie%' then 'IE'
            when lower(account_name) like '% it%' then 'IT'
            when lower(account_name) like '% nl%' then 'NL'
            when lower(account_name) like '% at%' then 'AT'
            when lower(account_name) like '% be%' then 'BE'
            when lower(account_name) like '% ch%' then 'CH'
            when lower(account_name) like '% es%' then 'ES'
            when lower(account_name) like '% no %' then 'NO'
            when lower(account_name) like '% fi %' then 'FI'
            when lower(account_name) like '% se %' then 'SE'
            when lower(account_name) like '% dk %' then 'DK'
            when lower(account_name) like '% mx %' then 'MX'
            when lower(account_name) like '% nz %' then 'NZ'
            when lower(account_name) like '% in %' then 'IN'
            when lower(account_name) like '%facebook%' and lower(account_name) like 'facebook -%' then 'US'
            when lower(account_name) = 'Facebook Video - Thruplay' then 'US'
            else 'Other Country'
            end as country,
case when tactic_granular = 'Display - Discovery' then concat('Touchpoint / Online display / DSC / ', case when audience = 'Pros' then 'Prospecting' else audience end)
when tactic_granular = 'Display - GDN' then concat('Touchpoint / Online display / GDA / ', case when audience = 'Pros' then 'Prospecting' else audience end)
when tactic_granular = 'Display - MSAN' then concat('Touchpoint / Online display / MSAN / ', case when audience = 'Pros' then 'Prospecting' else audience end)
end as touchpoint,
sum(coalesce(cost,0)) as cost,
sum(coalesce(impressions,0)) as impressions,
sum(coalesce(attr_gms_est,0)) as attr_gms_est,
sum(coalesce(attr_rev_est,0)) as attr_rev_est,
sum(coalesce(attr_receipts,0)) as attr_receipts_est,
sum(coalesce(attr_receipts_purch_date,0)) as attr_receipts_purch_date,
sum(coalesce(attr_gms_purch_date,0)) as attr_gms_purch_date,
sum(coalesce(attributed_rev_purch_date,0)) as attributed_rev_purch_date,
sum(coalesce(visits,0)) as visits
from etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker
where reporting_channel_group = 'Display'
and audience in ('CRM',"Pros","Retargeting")
group by 1,2,3,4),
mmm as 
(SELECT date(date_trunc(purchase_date, month)) as month, touchpoint, iso_country_code as country, sum(coalesce(gms,0)) as gms, sum(coalesce(spend,0)) as spend
FROM `etsy-data-warehouse-prod.rollups.neustar_mmm_data`  mmm
left join etsy-data-warehouse-prod.static.msts_countries c on mmm.key_market = c.country
where lower(touchpoint) like '%display%'
group by 1,2,3)
select coalesce(dt.month, mmm.month) as month, 
coalesce(dt.touchpoint, mmm.touchpoint) as touchpoint, 
coalesce(dt.country, mmm.country) as country, 
gms, 
spend, 
cost,
impressions,
attr_gms_est,
attr_rev_est,
attr_receipts_est,
attr_receipts_purch_date,
attr_gms_purch_date,
attributed_rev_purch_date,
visits
from daily_tracker dt
full outer join mmm using (month, touchpoint, country)
;
