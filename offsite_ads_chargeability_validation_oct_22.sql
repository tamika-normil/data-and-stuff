#checks the difference between the offsite_ads_chargeability in prod vs dev and also channel overview purch date
with osa_true as 
(SELECT order_date as order_date_true,
sum(coalesce(attr_gms,0)) as attributed_gms,
sum(coalesce(chargeable_gms,0)) as chargeable_gms,
sum(coalesce(advertising_revenue,0)) as advertising_revenue
FROM `etsy-data-warehouse-prod.rollups.offsite_ads_chargeability` 
where order_date >= '2020-02-04'
group by 1),
osa as 
(SELECT order_date,
sum(coalesce(attr_gms,0)) as attributed_gms,
sum(coalesce(chargeable_gms,0)) as chargeable_gms,
sum(coalesce(advertising_revenue,0)) as advertising_revenue
FROM `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` 
where order_date >= '2020-02-04'
group by 1),
co as 
(SELECT purchase_date as order_date,
sum(coalesce(attributed_gms,0)) as attributed_gms,
sum(coalesce(attributed_etsy_ads_revenue,0)) as advertising_revenue
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

with channel_overview as 
(SELECT purchase_date,
sum(coalesce(attributed_gms,0)) as attributed_gms,
sum(coalesce(attributed_receipts,0)) as attributed_receipts,	
sum(coalesce(attributed_gms_lw,0)) as attributed_gms_lw,
sum(coalesce(attributed_receipts_lw,0)) as attributed_receipts_lw,	
sum(coalesce(attributed_gms_ly,0)) as attributed_gms_ly,
sum(coalesce(attributed_receipts_ly,0)) as attributed_receipts_ly,	
sum(coalesce(attributed_gms_dly,0)) as attributed_gms_dly,
sum(coalesce(attributed_receipts_dly,0)) as attributed_receipts_dly,	
sum(coalesce(attributed_gms_dlly,0)) as attributed_gms_dlly,
sum(coalesce(attributed_receipts_dlly,0)) as attributed_receipts_dlly,	
sum(coalesce(attributed_gms_d3ly,0)) as attributed_gms_d3ly,
sum(coalesce(attributed_receipts_d3ly,0)) as attributed_receipts_d3ly,	
FROM etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted_purch_date
where (second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates'
                   ) or (second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%'))
          AND upper(utm_campaign) NOT LIKE '%_CUR_%'
AND purchase_date >= '2020-02-04'
group by 1),
rev as 
(SELECT purchase_date,
sum(coalesce(attributed_etsy_ads_revenue,0))  as attributed_attr_rev,	
sum(coalesce(attributed_etsy_ads_revenue_lw,0)) as attributed_attr_rev_lw,
sum(coalesce(attributed_etsy_ads_revenue_ly,0)) as attributed_attr_rev_ly,
sum(coalesce(attributed_etsy_ads_revenue_dly,0)) as attributed_attr_rev_dly,
sum(coalesce(attributed_etsy_ads_revenue_dlly,0)) as attributed_attr_rev_dlly,
sum(coalesce(attributed_etsy_ads_revenue_d3ly,0)) as attributed_attr_rev_d3ly,
FROM etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted_purch_date
where purchase_date >= '2020-02-04'
group by 1),
osa as 
(SELECT order_date,
sum(coalesce(attr_gms,0)),
sum(coalesce(attr_receipt,0)),	
sum(coalesce(attr_gms_lw,0)),
sum(coalesce(attr_receipt_lw,0)),	
sum(coalesce(attr_gms_ly,0)),
sum(coalesce(attr_receipt_ly,0)),	
sum(coalesce(attr_gms_dly,0)),
sum(coalesce(attr_receipt_dly,0)),	
sum(coalesce(attr_gms_dlly,0)),
sum(coalesce(attr_receipt_dlly,0)),	
sum(coalesce(attr_gms_d3ly,0)),
sum(coalesce(attr_receipt_d3ly,0)),		
sum(coalesce(advertising_revenue,0)),
sum(coalesce(advertising_revenue_lw,0)),
sum(coalesce(advertising_revenue_ly,0)),
sum(coalesce(advertising_revenue_dly,0)),
sum(coalesce(advertising_revenue_dlly,0)),
sum(coalesce(advertising_revenue_d3ly,0)),
FROM `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` 
where order_date >= '2020-02-04'
group by 1)
select a.*, 
attributed_attr_rev,	
attributed_attr_rev_lw,
attributed_attr_rev_ly,
attributed_attr_rev_dly,
attributed_attr_rev_dlly,
attributed_attr_rev_d3ly,
 b.*
from channel_overview a
left join rev r on a.purchase_date = r.purchase_date
left join osa b on a.purchase_date = b.order_date
;
