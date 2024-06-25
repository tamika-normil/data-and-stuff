-- perf_listing_agg rollup 


-- owner: tnormil@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: This rollup supports feeds performance reporting for Offsite Ads (OSA) Feeds monitoring and optimizations. 
-- dependencies: etsy-data-warehouse-prod.rollups.perf_listings, etsy-data-warehouse-prod.rollups.perf_listings_purchase_date, etsy-data-warehouse-prod.rollups.perf_listings_sum
-- access: etsy-data-warehouse-prod.rollups.perf_listings_agg=group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com

BEGIN

/*
Step 1: Declare which countries we'll have more granular aggregated metrics for. All other countries will be grouped as RoW.
*/

declare countries array<string> default [
     'AU', 'CA', 'DE', 'FR', 'US','GB', 'AT' ,'BE', 'CH', 'IT', 'ES', 'IN', 'NL'
    ];


/*
Step 2: Manually define channel dimensions and aggregate performance by our key dimensions. 

This is an old methodology for channel dimensions. 

It is best practice to join to etsy-data-warehouse-prod.buyatt_mart.channel_dimensions for channel dimensions.

In 2022, channel definitions were updated in weblog visits. Anytime there are changes in upstream data, etsy-data-warehouse-prod.rollups.perf_listings_sum needs to be backfilled to reflect the latest. Also, the fields that facitalite that join need to be added to etsy-data-warehouse-prod.rollups.perf_listings_sum

The backfill takes a significant amount of time and this is a lower priority rollup so we did not complete this action. Hence why we're relying on these manually defined channel dimensions.
*/

/*
Adding visit quality stats from buyatt_mart.visits table
*/ 

create or replace temp table visit_quality as 
	(	select _date as date1
  , regexp_extract(landing_event_url, r'/listing/([0-9]+)') as listing_id1
	, top_channel as top_channel1
	, second_channel as second_channel1
	, third_channel as third_channel1
	, marketing_region as marketing_region1
	, coalesce(utm_source, "") as utm_source1
  , coalesce(utm_medium, "") as utm_medium1
	, case when utm_campaign like '%\\_gdn%' or utm_campaign like '%gdn_%' then utm_campaign else "" end as utm_campaign1 
  , case when second_channel in ('affiliates') then utm_content else "" end as utm_content1
	, count(*) as total_visits
  , sum(bounced) as bounced_visits
  , round(avg(pages_seen)) as avg_pages_seen
	from `etsy-data-warehouse-prod.buyatt_mart.visits`
	where landing_event = 'view_listing'
	group by 1,2,3,4,5,6,7,8,9,10
	having listing_id1 is not null 
	);

create or replace temp table perf_listings as (
select p.*,
	v.*,
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
left join visit_quality v on
	safe_cast(p.listing_id as int64) = safe_cast(v.listing_id1 as int64) and p.date = v.date1 and p.top_channel = v.top_channel1 
	and p.second_channel = v.second_channel1 and p.third_channel = v.third_channel1 and
	p.marketing_region = v.marketing_region1 and p.utm_source = v.utm_source1 and p.utm_medium = v.utm_medium1
	and p.utm_campaign = v.utm_campaign1 and p.utm_content = v.utm_content1
where top_channel in ('us_paid','intl_paid'));


create or replace temp table perf_listings_agg as 
	(select date,
	marketing_region, 
	top_channel,
	reporting_channel_group,
	audience,
	tactic_high_level,
 	engine,
	listing_id,
	sum(first_page_visits) as visits,
	sum(first_page_attr_receipt) as attr_receipt,
	sum(first_page_attr_receipt_listing) as attr_receipt_listing,
	sum(first_page_attr_gms) as attr_gms,
	sum(first_page_attr_gms_listing) as attr_gms_listing, 
	sum(first_page_attr_rev) as attr_rev,
	sum(bounced_visits) / nullif(sum(total_visits),0) as bounce_rate,
	avg(avg_pages_seen) as average_pages_seen
	from perf_listings
	group by 1,2,3,4,5,6,7,8);

/*
Step 3: Aggregated metrics from external vendors
*/

create or replace temp table adwords as 
    (SELECT day as date, listing_id, market, 
		"PLA" as reporting_channel_group, 'Google - Paid' as engine, 
		'None/Other' as audience,'PLA - Manual' as tactic_high_level, case when market = 'US' then 'us_paid' else 'intl_paid' end as top_channel,
		sum(cost) as cost, sum(impressions) as impressions, sum(clicks) as clicks
    FROM `etsy-data-warehouse-prod.marketing.adwords_shopping_performance_report` 
    WHERE day >= "2019-01-01" and market in unnest(countries)
    group by 1,2,3,4,5);

/*
Step 4: Calculate aggregated metrics based on historical performance to date by region, channel, and listing, like number of clicks in the past 90 days and last click date prior to the respective date

For channels without external vendor data, we calculate metrics based on visits data. For channels with external vendor data, we calculate metrics based on clicks data from that external data set.
*/

create or replace temp table rolling_listings_market_perf as (with
ext as
        (select listing_id, 
        case when a.marketing_region in unnest(countries) then a.marketing_region
        else 'RoW' end as key_marketing_region, 
        date,
        reporting_channel_group,
        engine,
        sum(first_page_visits) as visits,
        from perf_listings  a
        where (reporting_channel_group in ('Display', 'Paid Social') or (reporting_channel_group in ('Affiliates') and tactic_high_level = 'Affiliates - Feed')
				or (reporting_channel_group in ('PLA') and engine <> 'Google - Paid')) 
        group by 1,2,3,4,5
				union all
				select safe_cast(listing_id as int64) as listing_id, 
        case when a.market in unnest(countries) then a.market
        else 'RoW' end as key_marketing_region, 
        date,
        reporting_channel_group,
        engine,
        sum(clicks) as visits,
        from adwords a
        group by 1,2,3,4,5),
past_ext as 
		(select d.listing_id, 
		d.date, 
		d.key_marketing_region,
		d.reporting_channel_group,
		d.engine,
		sum(e.visits) as past_quarter_clicks 
		from ext d
		left join ext e on d.listing_id = e.listing_id and d.key_marketing_region = e.key_marketing_region and d.reporting_channel_group = e.reporting_channel_group and d.engine = e.engine
		and e.date >= date_sub(d.date, interval 90 day)
		group by 1,2,3,4,5),
last_ext as 
		(select distinct listing_id, 
		date, 
		key_marketing_region,
		reporting_channel_group,
		engine,
		lag(date) over (partition by listing_id, key_marketing_region, reporting_channel_group, engine order by date asc) as last_clk_date
		from ext)
select a.listing_id, 
a.date, 
a.key_marketing_region,
a.reporting_channel_group,
a.engine,
a.past_quarter_clicks,
b.last_clk_date
from past_ext a
left join last_ext b using (date, listing_id, key_marketing_region, reporting_channel_group, engine)
);

/*
Step 5: Merge temp tables with performance data with static data sets that describe listings in more detail to create final data set
*/

create temp table perf_listings_agg_final as 
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
    qualify rank = 1)
SELECT coalesce(a.date, i.date) as date,
coalesce(f.overall_giftiness, g.overall_giftiness) as overall_giftiness,
case when coalesce(a.marketing_region, i.market) in unnest(countries) then coalesce(a.marketing_region, i.market) 
else 'RoW' end as marketing_region, 
coalesce(a.top_channel, i.top_channel) as top_channel,
coalesce(a.reporting_channel_group,i.reporting_channel_group) as reporting_channel_group,
coalesce(a.audience, i.audience) as audience,
coalesce(a.tactic_high_level, i.tactic_high_level) as tactic_high_level,
coalesce(a.engine,i.engine) as engine,
coalesce(a.listing_id,safe_cast(i.listing_id as int64)) as listing_id,
c.price_tier,
c.price,
c.top_category,
c.subcategory,
c.category,
c.is_bestseller,
c.seller_tier,
c.seller_tier_gpla, 
sum(coalesce(visits,0)) as visits,
sum(coalesce(attr_receipt,0)) as attr_receipt,
sum(coalesce(attr_receipt_listing,0)) as attr_receipt_listing,
sum(coalesce(attr_gms,0)) as attr_gms,
sum(coalesce(attr_gms_listing,0)) as attr_gms_listing, 
sum(coalesce(attr_rev,0) - coalesce(visits,0)*0.0063) as attr_rev,
sum(coalesce(cost,0)) as cost,
sum(coalesce(impressions,0)) as impressions,
sum(coalesce(clicks,0)) as clicks,
sum(coalesce(bounce_rate,0)) as bounced_rate,
avg(coalesce(average_pages_seen,0)) as average_pages_seen
FROM perf_listings_agg  a 
full outer join adwords i on a.date = i.date and a.listing_id = safe_cast(i.listing_id as int64) and a.marketing_region = i.market and a.reporting_channel_group = i.reporting_channel_group and a.engine = i.engine
left join static_perf_listing c on coalesce(a.listing_id,safe_cast(i.listing_id as int64)) = c.listing_id
left join `etsy-data-warehouse-prod.knowledge_base.listing_giftiness` f on coalesce(a.listing_id,safe_cast(i.listing_id as int64)) = f.listing_id 
and coalesce(a.date, i.date) = date(timestamp_seconds(f.run_date)) and f._date >= '2021-03-04' 
left join gifty_backfill g on coalesce(a.listing_id,safe_cast(i.listing_id as int64)) = g.listing_id
where coalesce(a.date, i.date) >= '2019-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17);

/* 

create or replace table `etsy-data-warehouse-dev.rollups.perf_listings_agg` 
partition by `date`
cluster by listing_id as 
(select a.*, h.past_quarter_clicks, date_diff(a.date, last_clk_date, DAY) as days_between_click,
from perf_listings_agg_final a
left join rolling_listings_market_perf h on a.date = h.date and a.listing_id = h.listing_id and a.marketing_region = h.key_marketing_region and a.reporting_channel_group = h.reporting_channel_group and a.engine = h.engine);

*/


drop table if exists `etsy-data-warehouse-dev.rollups.perf_listings_agg`;

create table `etsy-data-warehouse-dev.rollups.perf_listings_agg` 
partition by `date`
cluster by listing_id as 
(select a.*, h.past_quarter_clicks, date_diff(a.date, last_clk_date, DAY) as days_between_click,
from perf_listings_agg_final a
left join rolling_listings_market_perf h on a.date = h.date and a.listing_id = h.listing_id and a.marketing_region = h.key_marketing_region and a.reporting_channel_group = h.reporting_channel_group and a.engine = h.engine);

END
