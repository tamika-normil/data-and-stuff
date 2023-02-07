with bl_perf as 
(SELECT  date_trunc(date, month) as date, sum(visits) as visits, sum(attr_receipt) as attr_receipt, sum(attr_gms) as attr_gms, sum(attr_rev) as attr_rev, count(distinct listing_id) as listings
FROM etsy-data-warehouse-prod.rollups.perf_listings l
join `etsy-data-warehouse-dev.tnormil.seller_tags_blocklist` b using (listing_id)
where marketing_region = 'US'
and second_channel like '%gpla%'
and date < '2022-12-01'
group by 1),
perf as 
(SELECT  date_trunc(date, month) date, sum(visits) as visits, sum(attr_receipt) as attr_receipt, sum(attr_gms) as attr_gms, sum(attr_rev) as attr_rev, count(distinct listing_id) as listings
FROM etsy-data-warehouse-prod.rollups.perf_listings l
where marketing_region = 'US'
and second_channel like '%gpla%'
and date < '2022-12-01'
group by 1),
bl_shop as
(SELECT  date_trunc(day, month) as date, sum(clicks) as clicks, sum(impressions) as impressions
FROM `etsy-data-warehouse-prod.marketing.adwords_shopping_performance_report` l
join `etsy-data-warehouse-dev.tnormil.seller_tags_blocklist` b using (listing_id)
where market = 'US'
and day < '2022-12-01'
group by 1),
shop as 
(SELECT  date_trunc(day, month) date, sum(clicks) as clicks, sum(impressions) as impressions
FROM `etsy-data-warehouse-prod.marketing.adwords_shopping_performance_report` l
where market = 'US'
and day < '2022-12-01'
group by 1)
select p.date, p.visits, p.attr_receipt, p.attr_gms, p.attr_rev, p.listings, s.clicks, s.impressions,
 b.visits as bl_visits, b.attr_receipt as bl_attr_receipt, b.attr_gms as bl_attr_gms, b.attr_rev as bl_attr_rev, b.listings as bl_listings, bs.clicks as bs_clicks, bs.impressions as bl_impressions,
from perf p
left join bl_perf b using (date)
left join shop s using (date)
left join bl_shop bs using (date)
;
