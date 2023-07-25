BEGIN

create or replace temp table perf_listings as (
select p.*, 
	case 
		when top_channel in ('direct', 'dark', 'internal', 'seo') then initcap(top_channel)
		when top_channel like 'social_%' then 'Non-Paid Social'
		when top_channel like 'email%' then 'Email'
		when top_channel like 'push_%' then 'Push'
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA'
				when (second_channel like '%google_ppc' or second_channel like '%intl_ppc%' or second_channel like '%bing_ppc' or second_channel like 'admarketplace') then 'SEM'
				when second_channel='affiliates'  then 'Affiliates'
				when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
				when second_channel like '%native_display' or (utm_source = 'google' and utm_medium = 'cpc' and (utm_campaign like 'gdn%' or utm_campaign like 'gda%')) then 'Display'
				when second_channel in ('us_video','intl_video') then 'Video'
				else 'Other Paid'
			end
		else 'Other Non-Paid'
	end as reporting_channel_group, 
    case when utm_campaign like '%_rtg%' then 'Retargeting'
    when utm_campaign like '%_crm%' then 'CRM'
    when utm_campaign like '%_pros%' then 'Pros'
    else 'None/Other' end as audience,
    	case 
		when top_channel in ('direct', 'dark', 'internal', 'social_organic','seo','other_utms','other_referrer_no_utms') then 'N/A'
		when utm_medium= 'editorial_internal' then 'Owned Social - Blog'
		when top_channel like 'social_promoted' then 'Owned Social - Other'
		when top_channel like 'email' then 'Email - Marketing'
		when top_channel like 'email_transactional' then 'Email - Transactional'
		when top_channel like 'push_trans' then 'Push - Transactional'
		when top_channel like 'push_%' then 'Push - Marketing'
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%gpla' or second_channel like '%bing_plas') then 
				case when third_channel like '%_max' then 'PLA - Automatic'
				else 'PLA - Manual'
				end
			when (second_channel like '%_ppc' or second_channel like 'admarketplace') then 
				case when third_channel like '%_brand' then 'SEM - Brand'
				when third_channel = 'admarketplace' then 'SEM - Other'
				else 'SEM - Non-Brand' 
				end
			when second_channel='affiliates'  then 
				case when third_channel = 'affiliates_feed' then 'Affiliates - Feed'
				when third_channel = 'affiliates_widget' then 'Affiliates - Widget'
				else 'Affiliates - Other'
				end
			when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 
				case when third_channel like '%dynamic%' then 'Paid Social - Dynamic'
				when third_channel like '%curated%' then 'Paid Social - Curated'
				else 'Paid Social - Other'
				end
			when second_channel like '%native_display' then 'Display - Native'
			when second_channel in ('us_video','intl_video') then 
				case when third_channel like 'reserve_%' then 'Video - Reserved'
				else 'Video - Programmatic'
				end
			else 'Other'
			end
		else 'N/A'
		end as tactic_high_level,
      case 
		when top_channel in ('direct', 'dark', 'internal') then 'Direct/Dark/Internal'
		when (top_channel like 'email%' or top_channel like 'push_%') then 'Other - with UTMs'
		when top_channel = 'other_utms' then 'Other - with UTMs'
		when top_channel = 'other_referrer_no_utms' then 'Other - without UTMs'
		when top_channel in ('us_paid','intl_paid') then 
			case when (second_channel like '%google%' or second_channel like '%gpla%' or second_channel like '%intl_ppc%' or third_channel like '%google%' or third_channel like '%youtube%' or third_channel like '%dv360%') then 'Google - Paid'  
				when (second_channel like '%bing%' or third_channel like '%msan%' or third_channel like '%bing%') then 'Bing - Paid'
				when second_channel='affiliates'  then 'Affiliates'   
				when second_channel='admarketplace' then 'Admarketplace'
				when second_channel like '%css_plas' then 'Connexity'
				when (second_channel like 'facebook_disp%' or third_channel like '%facebook%') then 'Facebook - Paid'
				when (second_channel like 'pinterest_disp%' or third_channel like '%pinterest%') then 'Pinterest - Paid'
				else 'Other Paid'
			end
		when (second_channel like '%google%' or third_channel like '%google%') then 'Google - Organic'
		when (second_channel like '%bing%' or third_channel like '%bing%') then 'Bing - Organic'
		when (second_channel like 'social_o_facebook%'  or second_channel like 'social_o_instagram%' ) then 'Facebook/Instagram - Earned'
		when (second_channel like 'social_p_facebook%'  or second_channel like 'social_p_instagram%' ) then 'Facebook/Instagram - Owned'
		when second_channel like 'social_o_pinterest%'  then 'Pinterest - Earned'
		when second_channel like 'social_p_pinterest%'  then 'Pinterest - Owned'
		when second_channel like 'social_o_%' then 'Other Social - Earned'
		when second_channel like 'social_p_%' then 'Other Social - Owned'
		else 'Other Non-Paid'
	end as engine
from `etsy-data-warehouse-prod.rollups.perf_listings_sum` p
where top_channel in ('us_paid','intl_paid'));

create or replace temp table rolling_listings_market_perf as (with
listings as     
        (select listing_id, 
        case when a.marketing_region in ('AU', 'CA', 'DE', 'FR', 'US','GB', 'IN') then a.marketing_region
        else 'RoW' end as key_marketing_region, 
        reporting_channel_group,
        engine,
        min(date) as first_clk_date,
        from perf_listings  a
        where first_page_visits > 0 and (reporting_channel_group in ('PLA', 'Display', 'Paid Social') or (reporting_channel_group in ('Affiliates') and tactic_high_level = 'Affiliates - Feed')) 
        group by 1, 2, 3,4),
listing_dates as 
        (SELECT date, l.listing_id, key_marketing_region, reporting_channel_group, engine, first_clk_date
        FROM `etsy-data-warehouse-prod.listing_mart.listing_vw` l
        join listings li using (listing_id)
        join UNNEST(GENERATE_DATE_ARRAY(li.first_clk_date, current_date() - 1, INTERVAL 1 DAY)) AS date 
        where date >= '2020-01-01'),
ext as
        (select listing_id, 
        case when a.marketing_region in ('AU', 'CA', 'DE', 'FR', 'US','GB', 'IN') then a.marketing_region
        else 'RoW' end as key_marketing_region, 
        date,
        reporting_channel_group,
        engine,
        sum(first_page_visits) as visits,
        from perf_listings  a
        where (reporting_channel_group in ('PLA', 'Display', 'Paid Social') or (reporting_channel_group in ('Affiliates') and tactic_high_level = 'Affiliates - Feed')) 
        group by 1, 2, 3, 4,5 )               
select d.listing_id, 
d.date, 
d.key_marketing_region,
first_clk_date,
d.reporting_channel_group,
d.engine,
sum(coalesce(visits,0)) over (partition by d.listing_id, d.key_marketing_region, d.reporting_channel_group, d.engine order by d.date asc rows between 90 PRECEDING AND 1 PRECEDING) as past_quarter_clicks,
max(e.date) over (partition by d.listing_id, d.key_marketing_region, d.reporting_channel_group, d.engine order by d.date asc rows between 90 PRECEDING AND 1 PRECEDING) as last_clk_date
from listing_dates d
left join ext e on d.date = e.date and d.listing_id = e.listing_id and d.key_marketing_region = e.key_marketing_region and d.reporting_channel_group = e.reporting_channel_group and d.engine = e.engine
);


create or replace table `etsy-data-warehouse-dev.rollups.perf_listings_agg` as 
(with static_perf_listing  as (with 
shipping_profiles as
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
when pt.category is not null and l.price/100 < pt.d and  l.price >= pt.e then 'd'
when pt.category is not null and  l.price/100 < pt.c and  l.price >= pt.d then 'c'
when pt.category is not null and  l.price/100 < pt.b and  l.price >= pt.c then 'b'
when pt.category is not null and l. price/100 >= pt.a then 'a'
when pt.category is null and  l.price/100 < 6.50 then 'e'
when pt.category is null and  l.price/100 < 13 and  l.price >= 6.50 then 'd'
when pt.category is null and  l.price/100 < 22 and  l.price >= 13 then 'c' 
when pt.category is null and  l.price/100 < 40 and  l.price >= 22 then 'b' 
when pt.category is null and  l.price/100 >= 40 then 'a' end as price_tier,
l.price/100 as price,
case when sp.listing_id is not null then 1 else 0 end as  rts,
case when l.color is not null then 1 else 0 end as has_color,
score as nsfw_score,
is_download,
olf.category,
olf.is_bestseller,
olf.seller_tier,
olf.seller_tier_gpla
from `etsy-data-warehouse-prod.listing_mart.listing_vw` l
left join `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` c on l.listing_id = c.listing_id
left join `etsy-data-warehouse-prod.rollups.seller_basics` s on l.shop_id = s.shop_id
left join `etsy-data-warehouse-prod.static.price_tier` pt on l.top_category = pt.category
left join  `etsy-sr-etl-prod.nsfw_score.inferenced_all_listings` nsfw on l.listing_id = nsfw.listing_id
left join listing_shipping_profiles sp on l.listing_id = sp.listing_id
left join `etsy-data-warehouse-prod.olf.olf_hydration_daily` olf on l.listing_id = olf.listing_id 
and DATE(olf._PARTITIONTIME) = current_date - 1 ),
gifty_backfill as
    (SELECT listing_id, overall_giftiness, row_number() over (partition by listing_id order by date(timestamp_seconds(run_date)) asc) as rank
    from `etsy-data-warehouse-prod.knowledge_base.listing_giftiness` 
    where _date >= '2021-03-04'
    qualify rank = 1),
num_range as 
    (SELECT start, start + 20 as endnum, past_quarter_clicks
    FROM UNNEST(GENERATE_ARRAY(1,3000,20)) AS start
    join UNNEST(GENERATE_ARRAY(1,3000,1)) AS past_quarter_clicks
    where past_quarter_clicks between start and start + 20),
adwords as 
    (SELECT day, listing_id, market, "PLA" as reporting_channel_group, 'Google - Paid' as engine, sum(cost) as cost, sum(impressions) as impressions, sum(clicks) as clicks
    FROM `etsy-data-warehouse-prod.marketing.adwords_shopping_performance_report` 
    WHERE day >= "2019-01-01" and market in ('AU', 'CA', 'DE', 'FR', 'US','GB', 'AT' ,'BE', 'CH', 'IT', 'ES', 'IN')
    group by 1,2,3,4)
SELECT a.date,
coalesce(f.overall_giftiness, g.overall_giftiness) as overall_giftiness,
case when a.marketing_region in ('AU', 'CA', 'DE', 'FR', 'US','GB', 'AT', 'CH', 'BE', 'IT', 'ES', 'IN') then a.marketing_region
else 'RoW' end as marketing_region, 
a.top_channel,
a.reporting_channel_group,
a.audience,
a.tactic_high_level,
a.engine,
a.listing_id,
c.price_tier,
c.price,
c.top_category,
c.subcategory,
c.category,
c.is_bestseller,
c.seller_tier,
c.seller_tier_gpla,
h.past_quarter_clicks,
date_diff(a.date, last_clk_date, DAY) as days_between_click, 
sum(first_page_visits) as visits,
sum(first_page_attr_receipt) as attr_receipt,
sum(first_page_attr_receipt_listing) as attr_receipt_listing,
sum(first_page_attr_gms) as attr_gms,
sum(first_page_attr_gms_listing) as attr_gms_listing, 
sum(first_page_attr_rev) as attr_rev,
sum(cost) as cost,
sum(impressions) as impressions,
sum(clicks) as clicks
FROM perf_listings  a 
left join static_perf_listing c on a.listing_id = c.listing_id
left join `etsy-data-warehouse-prod.knowledge_base.listing_giftiness` f on a.listing_id = f.listing_id 
and a.date = date(timestamp_seconds(f.run_date)) and f._date >= '2021-03-04' 
left join gifty_backfill g on a.listing_id = g.listing_id
left join rolling_listings_market_perf h on a.date = h.date and a.listing_id = h.listing_id and a.marketing_region = h.key_marketing_region and a.reporting_channel_group = h.reporting_channel_group and a.engine = h.engine
left join adwords i on a.date = i.day and a.listing_id = i.listing_id and a.marketing_region = i.market and a.reporting_channel_group = i.reporting_channel_group and a.engine = i.engine
where a.date >= '2019-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19);

END
