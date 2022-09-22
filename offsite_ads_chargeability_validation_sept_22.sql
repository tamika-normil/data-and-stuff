#checks the difference between the offsite_ads_chargeability in prod vs dev and also channel overview purch date
with osa_true as 
(SELECT order_date as order_date_true,
sum(coalesce(attr_gms,0)) as attributed_gms,
sum(coalesce(chargeable_gms,0)) as chargeable_gms,
FROM `etsy-data-warehouse-prod.rollups.offsite_ads_chargeability` 
where order_date >= '2020-02-04'
group by 1),
osa as 
(SELECT order_date,
sum(coalesce(attr_gms,0)) as attributed_gms,
sum(coalesce(chargeable_gms,0)) as chargeable_gms,
FROM `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` 
where order_date >= '2020-02-04'
group by 1),
co as 
(SELECT purchase_date as order_date,
sum(coalesce(attributed_gms,0)) as attributed_gms,
FROM etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted_purch_date
where (second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates'
                   ) or (second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%'))
          AND upper(utm_campaign) NOT LIKE '%_CUR_%'
AND purchase_date >= '2020-02-04'
group by 1)
select a.*, b.*, co.*
from osa_true a
left join osa b on a.order_date_true = b.order_date
left join co on a.order_date_true = co.order_date
;

#checks the difference between the offsite_ads_chargeability dev and channel overview purch date to validate the YOY calcs
with channel_overview as 
(SELECT purchase_date,
sum(coalesce(attributed_gms,0)) as attributed_gms,
sum(coalesce(attributed_receipts,0)) as attributed_receipts,	
#sum(coalesce(attributed_attr_rev,0) - coalesce(prolist_revenue,0))  as attributed_attr_rev,
sum(coalesce(attributed_gms_lw,0)) as attributed_gms_lw,
sum(coalesce(attributed_receipts_lw,0)) as attributed_receipts_lw,	
#sum(coalesce(attributed_attr_rev_lw,0) - coalesce(prolist_revenue_lw,0)) as attributed_attr_rev_lw,
sum(coalesce(attributed_gms_ly,0)) as attributed_gms_ly,
sum(coalesce(attributed_receipts_ly,0)) as attributed_receipts_ly,	
#sum(coalesce(attributed_attr_rev_ly,0) - coalesce(prolist_revenue_ly,0)) as attributed_attr_rev_ly,
sum(coalesce(attributed_gms_dly,0)) as attributed_gms_dly,
sum(coalesce(attributed_receipts_dly,0)) as attributed_receipts_dly,	
#sum(coalesce(attributed_attr_rev_dly,0) - coalesce(prolist_revenue_dly,0)) as attributed_attr_rev_dly,
sum(coalesce(attributed_gms_dlly,0)) as attributed_gms_dlly,
sum(coalesce(attributed_receipts_dlly,0)) as attributed_receipts_dlly,	
#sum(coalesce(attributed_attr_rev_dlly,0) - coalesce(prolist_revenue_dlly,0)) as attributed_attr_rev_dlly,
sum(coalesce(attributed_gms_d3ly,0)) as attributed_gms_d3ly,
sum(coalesce(attributed_receipts_d3ly,0)) as attributed_receipts_d3ly,	
#sum(coalesce(attributed_attr_rev_d3ly,0) - coalesce(prolist_revenue_d3ly,0)) as attributed_attr_rev_d3ly,
FROM etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted_purch_date
where (second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates'
                   ) or (second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%'))
          AND upper(utm_campaign) NOT LIKE '%_CUR_%'
AND purchase_date >= '2020-02-04'
group by 1),
osa as 
(SELECT order_date,
sum(coalesce(attr_gms,0)),
sum(coalesce(attr_receipt,0)),	
#sum(coalesce(attr_rev,0)),
sum(coalesce(attr_gms_lw,0)),
sum(coalesce(attr_receipt_lw,0)),	
#sum(coalesce(attr_rev_lw,0)),
sum(coalesce(attr_gms_ly,0)),
sum(coalesce(attr_receipt_ly,0)),	
#sum(coalesce(attr_rev_ly,0)),
sum(coalesce(attr_gms_dly,0)),
sum(coalesce(attr_receipt_dly,0)),	
#sum(coalesce(attr_rev_dly,0)),
sum(coalesce(attr_gms_dlly,0)),
sum(coalesce(attr_receipt_dlly,0)),	
#sum(coalesce(attr_rev_dlly,0)),
sum(coalesce(attr_gms_d3ly,0)),
sum(coalesce(attr_receipt_d3ly,0)),	
#sum(coalesce(attr_rev_d3ly,0)),
FROM `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` 
where order_date >= '2020-02-04'
group by 1)
select a.*, b.*
from channel_overview a
left join osa b on a.purchase_date = b.order_date
;

# queries for visit level investigation below
select order_date, 
visit_date,  
top_channel,
second_channel,
utm_campaign,
coalesce(utm_custom2,'') as utm_custom2,
category,
canonical_region,
mapped_region,
device,
seller_tier
from `etsy-data-warehouse-prod.rollups.offsite_ads_chargeability` 
where order_date < '2022-09-21'
except distinct 
select order_date, 
visit_date,  
top_channel,
second_channel,
utm_campaign,
utm_custom2,
category,
canonical_region,
mapped_region,
device,
seller_tier
from `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` 
order by order_date, 
visit_date,  
top_channel,
second_channel,
utm_campaign,
category,
canonical_region,
mapped_region,
device,
seller_tier;

with osa_true as 
(select order_date, 
visit_date,  
top_channel,
second_channel,
utm_campaign,
coalesce(utm_custom2,'') as utm_custom2,
category,
canonical_region,
mapped_region,
device,
seller_tier,
sum(attr_gms) as attr_gms,
sum(chargeable_gms) as chargeable_gms
from `etsy-data-warehouse-prod.rollups.offsite_ads_chargeability` 
where order_date < '2022-09-21'
group by 1,2,3,4,5,6,7,8,9,10,11),
osa  as
(select order_date, 
visit_date,  
top_channel,
second_channel,
utm_campaign,
utm_custom2,
category,
canonical_region,
mapped_region,
device,
seller_tier,
sum(attr_gms) as attr_gms
from `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` 
where order_date < '2022-09-21'
group by 1,2,3,4,5,6,7,8,9,10,11)
select osa_true.*, osa.*
#case when utm_custom2 is null then 1 else 0 end, count(*)
from osa_true
left join osa using (order_date, 
visit_date,  
top_channel,
second_channel,
utm_campaign,
utm_custom2,
category,
canonical_region,
mapped_region,
device,
seller_tier)
where round(osa.attr_gms) <> round(osa_true.attr_gms)
limit 100;
