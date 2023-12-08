-- check p date kpis
with s3_data as
(SELECt date_date,
channel,
sum(attributed_gms_purch_date) as attributed_gms_purchase_date,
sum(attributed_receipts_purch_date) as attributed_receipts_purch_date,
sum(attributed_rev_purch_date) as attributed_rev_purchase_date
FROM `etsy-data-warehouse-dev.buyatt_mart.perf_marketing_s3_data`
#utm_campaign_cleaned as utm_campaign, 
group by 1,2
order by 1 desc),
truth as
(SELECT purchase_date as date_date, 
case when second_channel in ('google_ppc','bing_ppc','intl_bing_ppc','intl_ppc') and (lower(utm_campaign) like '%etsy%' or 
			lower(utm_campaign) like '%brand%') and lower(utm_campaign)  not like '%nonbrand%' then 'Brand'
			when second_channel in ('native_display','intl_native_display') then 'Display'
			when second_channel in ('google_ppc','bing_ppc','intl_bing_ppc','intl_ppc') then 'Non-Brand'
			when second_channel in ('gpla','bing_plas','intl_gpla','intl_bing_plas') then 'PLA'
			when second_channel in ('facebook_disp','instagram_disp','pinterest_disp','pinterest_disp_intl','facebook_disp_intl') then 'Social'
			else 'Other' 
		end as channel,
sum(attributed_gms) as attributed_gms_purchase_date,
sum(attributed_receipts) as attributed_receipts_purch_date,
sum(attributed_attr_rev) as attributed_rev_purchase_date
FROM  `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date` a
WHERE purchase_date>= DATE_SUB(current_date(), INTERVAL 36 DAY)
and (a.second_channel in ('facebook_disp','instagram_disp','facebook_disp_intl','gpla', 'google_ppc', 'bing_ppc', 'css_plas', 'intl_css_plas',
	'bing_plas','intl_gpla','intl_ppc','intl_bing_ppc','intl_bing_plas','pinterest_disp','pinterest_disp_intl','us_video','intl_video', 'native_display', 'intl_native_display')
	or (a.utm_source='admarketplace' and a.utm_medium='cpc'))
# utm_campaign,
group by 1,2
order by 1 desc)
select truth.*, s3_data.*
from truth
left join s3_data using (date_date, channel);

-- compare dev vs prod perf marketing s3
with s3_data as
(SELECt date_date,
channel,
sum(attributed_gms_purch_date) as attributed_gms_purchase_date,
sum(attributed_receipts_purch_date) as attributed_receipts_purch_date,
sum(attributed_rev_purch_date) as attributed_rev_purchase_date
FROM `etsy-data-warehouse-dev.buyatt_mart.perf_marketing_s3_data`
#utm_campaign_cleaned as utm_campaign, 
group by 1,2
order by 1 desc),
truth as
(SELECt date_date,
channel,
sum(attributed_gms_purch_date) as attributed_gms_purchase_date,
sum(attributed_receipts_purch_date) as attributed_receipts_purch_date,
sum(attributed_rev_purch_date) as attributed_rev_purchase_date
FROM `etsy-data-warehouse-prod.buyatt_mart.perf_marketing_s3_data`
#utm_campaign_cleaned as utm_campaign, 
group by 1,2
order by 1 desc)
select truth.*, s3_data.*
from truth
left join s3_data using (date_date, channel);

-- check click date kpis
with s3_data as
(SELECt date_date as date,
channel,
	sum(a.visits_m) as visits_m,
	sum(a.attributed_gms) as attributed_gms,
	sum(a.attributed_gms_estimated) as attributed_gms_estimated,
	sum(a.attributed_receipts) as attributed_receipts,
	sum(a.attributed_receipts_estimated) as attributed_receipts_estimated,
	sum(a.attributed_rev) as attributed_rev,
	sum(a.attributed_rev_estimated) as attributed_rev_estimated,
FROM `etsy-data-warehouse-dev.buyatt_mart.perf_marketing_s3_data` a 
#utm_campaign_cleaned as utm_campaign, 
group by 1,2
order by 1 desc),
truth as
(SELECT date, 
case when second_channel in ('google_ppc','bing_ppc','intl_bing_ppc','intl_ppc') and (lower(utm_campaign) like '%etsy%' or 
			lower(utm_campaign) like '%brand%') and lower(utm_campaign)  not like '%nonbrand%' then 'Brand'
			when second_channel in ('native_display','intl_native_display') then 'Display'
			when second_channel in ('google_ppc','bing_ppc','intl_bing_ppc','intl_ppc') then 'Non-Brand'
			when second_channel in ('gpla','bing_plas','intl_gpla','intl_bing_plas') then 'PLA'
			when second_channel in ('facebook_disp','instagram_disp','pinterest_disp','pinterest_disp_intl','facebook_disp_intl') then 'Social'
			else 'Other' 
		end as channel,
sum(visits) as visits,
sum(attributed_gms) as attributed_gms,
sum(attributed_gms_adjusted) as attributed_gms_adjusted,
sum(attributed_receipts) as attributed_receipts,
sum(attributed_receipts_adjusted) as attributed_receipts_adjusted,
sum(attributed_attr_rev) as attributed_attr_rev,
sum(attributed_attr_rev_adjusted) as attributed_attr_rev_adjusted
FROM  `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
WHERE date>= DATE_SUB(current_date(), INTERVAL 31 DAY)
and (a.second_channel in ('facebook_disp','instagram_disp','facebook_disp_intl','gpla', 'google_ppc', 'bing_ppc', 'css_plas', 'intl_css_plas',
	'bing_plas','intl_gpla','intl_ppc','intl_bing_ppc','intl_bing_plas','pinterest_disp','pinterest_disp_intl','us_video','intl_video', 'native_display', 'intl_native_display')
	or (a.utm_source='admarketplace' and a.utm_medium='cpc'))
# utm_campaign,
group by 1,2
order by 1 desc),
truth_gc as 
(SELECT date, 
case when second_channel in ('google_ppc','bing_ppc','intl_bing_ppc','intl_ppc') and (lower(utm_campaign) like '%etsy%' or 
			lower(utm_campaign) like '%brand%') and lower(utm_campaign)  not like '%nonbrand%' then 'Brand'
			when second_channel in ('native_display','intl_native_display') then 'Display'
			when second_channel in ('google_ppc','bing_ppc','intl_bing_ppc','intl_ppc') then 'Non-Brand'
			when second_channel in ('gpla','bing_plas','intl_gpla','intl_bing_plas') then 'PLA'
			when second_channel in ('facebook_disp','instagram_disp','pinterest_disp','pinterest_disp_intl','facebook_disp_intl') then 'Social'
			else 'Other' 
		end as channel,
sum(attributed_attr_rev_giftcards) as attributed_attr_rev,
sum(attributed_attr_rev_adjusted) as attributed_attr_rev_adjusted
FROM  etsy-data-warehouse-prod.buyatt_rollups.channel_overview_giftcards a
WHERE date>= DATE_SUB(current_date(), INTERVAL 31 DAY)
and (a.second_channel in ('facebook_disp','instagram_disp','facebook_disp_intl','gpla', 'google_ppc', 'bing_ppc', 'css_plas', 'intl_css_plas',
	'bing_plas','intl_gpla','intl_ppc','intl_bing_ppc','intl_bing_plas','pinterest_disp','pinterest_disp_intl','us_video','intl_video', 'native_display', 'intl_native_display')
	or (a.utm_source='admarketplace' and a.utm_medium='cpc'))
# utm_campaign,
group by 1,2
order by 1 desc),
truth_merge as 
(select coalesce(a.date, b.date) as date,
coalesce(a.channel, b.channel) as channel,
visits,
attributed_gms,
attributed_gms_adjusted,
attributed_receipts,
attributed_receipts_adjusted,
coalesce(a.attributed_attr_rev,0) + coalesce(b.attributed_attr_rev,0) as attributed_attr_rev,
coalesce(a.attributed_attr_rev_adjusted,0) + coalesce(b.attributed_attr_rev_adjusted,0) as attributed_attr_rev_adjusted
from truth_gc a
full outer join truth b using (date, channel))
select truth.*, s3_data.*
from truth_merge truth
left join s3_data using (date, channel);
