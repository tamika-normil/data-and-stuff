CREATE OR REPLACE TEMPORARY TABLE static_perf_listing AS 
(with shipping_profiles as
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
when pt.category is not null and l.price/100 < pt.d and l.price >= pt.e then 'd'
when pt.category is not null and l.price/100 < pt.c and l.price >= pt.d then 'c'
when pt.category is not null and l.price/100 < pt.b and l.price >= pt.c then 'b'
when pt.category is not null and l.price/100 >= pt.a then 'a'
when pt.category is null and l.price/100 < 6.50 then 'e'
when pt.category is null and l.price/100 < 13 and l.price >= 6.50 then 'd'
when pt.category is null and l.price/100 < 22 and l.price >= 13 then 'c' 
when pt.category is null and l.price/100 < 40 and l.price >= 22 then 'b' 
when pt.category is null and l.price/100 >= 40 then 'a' end as price_tier,
L.price/100 as price,
case when sp.listing_id is not null then 1 else 0 end as  rts,
case when l.color is not null then 1 else 0 end as has_color,
score as nsfw_score,
is_download,
seller_tier_gpla
from `etsy-data-warehouse-prod.listing_mart.listing_vw` l
left join `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` c on l.listing_id = c.listing_id
left join `etsy-data-warehouse-prod.rollups.seller_basics` s on l.shop_id = s.shop_id
left join `etsy-data-warehouse-prod.static.msts_countries` i on s.country_name = i.country
left join `etsy-data-warehouse-prod.static.price_tier` pt on l.top_category = pt.category
left join  `etsy-sr-etl-prod.nsfw_score.inferenced_all_listings` nsfw on l.listing_id = nsfw.listing_id
left join listing_shipping_profiles sp on l.listing_id = sp.listing_id 
left join `etsy-data-warehouse-prod.olf.olf_hydration_daily` olf on l.listing_id = olf.listing_id 
and DATE(olf._PARTITIONTIME) = current_date - 1);

with perf as 
    (select date_trunc(date, week) as date, 
    reporting_channel_group,
    marketing_region,
    split(engine, ' ')[safe_offset(0)] as vendor, 
    count(distinct listing_id) as clicked_listings
    from`etsy-data-warehouse-dev.rollups.perf_listings_sample_af` 
    group by 1,2,3,4),
fb_exclude as 
    (SELECT DATE(_PARTITIONTIME) as date, listing_id, illegal.value,
              split(illegal_term.key,'.')[SAFE_OFFSET(1)] as region,
              split(illegal_term.key,'.')[SAFE_OFFSET(2)] as language,
              array_to_string(value.value,'') as term,
              value.key as blocklist,
        FROM `etsy-data-warehouse-prod.olf.olf_hydration_daily` olf
                LEFT JOIN
            UNNEST(has_illegal_terms) illegal
                WITH
                    OFFSET
                        LEFT
                JOIN
            UNNEST(illegal_terms) illegal_term
                WITH
                    OFFSET
                        USING
        (
            OFFSET
        )
        LEFT JOIN UNNEST(illegal_term.value) value
        WHERE DATE(_PARTITIONTIME) >= '2022-05-01'
        and listing_state = 'is_active'
        and value.key = 'olf facebook exclusions'
        and illegal.value = true),
olf as
    (select date_trunc(DATE(_PARTITIONTIME), week) as date,
    country as marketing_region,
    vendor,
    count(distinct case when listing_state = 'is_active' then olf.listing_id end) as active_listings,
    count(distinct case when listing_state = 'is_active' and syndicated = true then olf.listing_id end) as syndicated_listings,
    count(distinct case when listing_state = 'is_active' and syndicated = true and filtered = false then olf.listing_id end) as filtered_listings,
   count(distinct case when listing_state = 'is_active' and syndicated = true and filtered = false and seller_tier_gpla in ('power_seller', 'top_seller_high') then olf.listing_id end) as est_filtered_listings_display_google,     
    count(distinct case when listing_state = 'is_active' and syndicated = true and filtered = false and seller_tier_gpla in ('power_seller') then olf.listing_id end) as est_filtered_listings_display_bing,      
    count(distinct case when listing_state = 'is_active' and syndicated = true and filtered = false and (fb.listing_id is null) then olf.listing_id end) as filtered_listings_fb,
    count(distinct case when listing_state = 'is_active' and syndicated = true and filtered = false and (fb.listing_id is null) and price_tier not in ('d','e') and seller_tier_gpla in ('power_seller', 'top_seller_high', 'top_seller_low') then olf.listing_id end) as est_filtered_listings_fb,  
    count(distinct case when listing_state = 'is_active' and syndicated = true and filtered = false and seller_tier_gpla in ('power_seller', 'top_seller_high', 'top_seller_low') then olf.listing_id end) as est_filtered_listings_pin    
    from `etsy-data-warehouse-prod.olf.listing_fact_daily` olf
    left join fb_exclude fb on olf.listing_id = fb.listing_id
    and date(olf._PARTITIONTIME) = fb.date
    left join static_perf_listing p on olf.listing_id = p.listing_id
    WHERE DATE(_PARTITIONTIME) >= '2022-05-01'
    group by 1,2,3)  
select coalesce(c.date, olf.date) as date,
c.reporting_channel_group,
c.vendor, 
coalesce(c.marketing_region, olf.marketing_region) as marketing_region,
active_listings,
syndicated_listings,
case when c.vendor = "Facebook" then filtered_listings_fb else filtered_listings end as filtered_listings,
case when c.reporting_channel_group = 'Display' and c.vendor in ('Google') then est_filtered_listings_display_google
when c.reporting_channel_group = 'Display' and c.vendor in ('Bing') then est_filtered_listings_display_bing
when c.vendor in ("Facebook") then est_filtered_listings_fb
when c.vendor in ("Pinterest") then est_filtered_listings_pin 
else filtered_listings end as est_filtered_listings,
clicked_listings,
from perf c
full outer join olf on c.marketing_region = olf.marketing_region
and c.date = olf.date
