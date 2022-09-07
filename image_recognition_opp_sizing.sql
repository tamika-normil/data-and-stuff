with bad_listings as 
(SELECT distinct listing_id 
FROM `etsy-data-warehouse-prod.listing_mart.listing_vw` 
left join `etsy-data-warehouse-prod.etsy_shard.listing_images` using (listing_id)
left join `etsy-data-warehouse-prod.etsy_shard.image_recognition_safe_search_annotations` using (image_id)
where (adult_likelihood >= 5 or racy_likelihood >= 5)),
bad_listings_impact as 
(SELECT reporting_channel_group, engine, date_trunc(date, month) as month, count(distinct listing_id) as listings,sum(attr_gms) as attr_gms,sum(attr_rev) as attr_rev
FROM `etsy-data-warehouse-prod.rollups.perf_listings_agg`
join bad_listings using (listing_id)
group by 1,2,3),
total as 
(SELECT reporting_channel_group, engine, date_trunc(date, month) as month, count(distinct listing_id) as listings,sum(attr_gms) as attr_gms,sum(attr_rev) as attr_rev
FROM `etsy-data-warehouse-prod.rollups.perf_listings_agg`
group by 1,2,3)
select a.reporting_channel_group, a.engine, a.month, a.listings, a.attr_gms, a.attr_rev, 
b.listings as bad_listings, b.attr_gms as bad_attr_gms, b.attr_rev as bad_attr_rev
from total a
left join bad_listings_impact b using (reporting_channel_group, engine, month)
