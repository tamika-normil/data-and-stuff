create or replace table `etsy-data-warehouse-dev.static.de_vampire_listings` as 

with base0 as 
(

select
marketing_region,
b.listing_id,
sum(b.impressions) as impressions,
sum(b.cost) as cost, 
sum(b.clicks) as clicks,
sum(b.attr_rev) as attr_rev
from  `etsy-data-warehouse-prod.rollups.perf_listings_agg` b
where b.marketing_region = 'DE' and b.reporting_channel_group = 'PLA'
and date between date_sub(current_date, interval 390 day) and date_sub(current_date, interval 30 day)
group by 1,2),


base1 as 
(

select
marketing_region,
b.listing_id,
sum(b.impressions) as impressions,
sum(b.cost) as cost, 
sum(b.clicks) as clicks,
sum(b.attr_rev) as attr_rev
from  `etsy-data-warehouse-prod.rollups.perf_listings_agg` b
where b.marketing_region = 'DE' and b.reporting_channel_group = 'PLA'
and date between date_sub(current_date, interval 60 day) and date_sub(current_date, interval 30 day)
group by 1,2),


ouput as (

select
a.listing_id, 
a.marketing_region,
sum(a.clicks) as one_year_clicks,  
sum(a.attr_rev) as one_year_revenue, 
sum(a.cost) as one_year_cost, 
sum(a.impressions) as one_year_impressions,
sum(b.clicks) as one_month_clicks,  
sum(b.attr_rev) as one_month_revenue, 
sum(b.cost) as one_month_cost, 
sum(b.impressions) as one_month_impressions,
from base0 a 
inner join base1 b on a.listing_id = b.listing_id
group by 1,2)

SELECT
    CASE 
        WHEN one_year_clicks BETWEEN 0 AND 9 THEN   'a. 0-9'
        WHEN one_year_clicks BETWEEN 10 AND 19 THEN 'b. 10-19'
        WHEN one_year_clicks BETWEEN 20 AND 29 THEN 'c. 20-29'
        WHEN one_year_clicks BETWEEN 30 AND 39 THEN 'd. 30-39'
        WHEN one_year_clicks BETWEEN 40 AND 49 THEN 'e. 40-49'
        WHEN one_year_clicks BETWEEN 50 AND 59 THEN 'f. 50-59'
        WHEN one_year_clicks BETWEEN 60 AND 69 THEN 'g. 60-69'
        WHEN one_year_clicks BETWEEN 70 AND 79 THEN 'h. 70-79'
        WHEN one_year_clicks BETWEEN 80 AND 89 THEN 'i. 80-89'
        WHEN one_year_clicks BETWEEN 90 AND 99 THEN 'j. 90-99'
        WHEN one_year_clicks >= 100 THEN  'k. 100+'
        ELSE 'unknown'
    END AS click_volume_bins,
    CASE 
        WHEN one_year_cost <= 0 THEN 'No Spend'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0 AND 0.1) THEN '0.0-0.1'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.1 AND 0.2) THEN '0.1-0.2'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.2 AND 0.3) THEN '0.2-0.3'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.3 AND 0.4) THEN '0.3-0.4'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.4 AND 0.5) THEN '0.4-0.5'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.5 AND 0.6) THEN '0.5-0.6'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.6 AND 0.7) THEN '0.6-0.7'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.7 AND 0.8) THEN '0.7-0.8'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.8 AND 0.9) THEN '0.8-0.9'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 0.9 AND 1.0) THEN '0.9-1.0'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 1.0 AND 1.1) THEN '1.0-1.1'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 1.1 AND 1.2) THEN '1.1-1.2'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 1.2 AND 1.3) THEN '1.2-1.3'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 1.3 AND 1.4) THEN '1.3-1.4'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost BETWEEN 1.4 AND 1.5) THEN '1.4-1.5'
        WHEN (one_year_cost > 0 AND one_year_revenue/one_year_cost > 1.5) THEN '1.5+'
        ELSE 'unknown'
    END AS one_year_ROI_bins,
    CASE 
        WHEN one_month_cost <= 0 THEN 'No Spend'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0 AND 0.1) THEN '0.0-0.1'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.1 AND 0.2) THEN '0.1-0.2'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.2 AND 0.3) THEN '0.2-0.3'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.3 AND 0.4) THEN '0.3-0.4'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.4 AND 0.5) THEN '0.4-0.5'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.5 AND 0.6) THEN '0.5-0.6'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.6 AND 0.7) THEN '0.6-0.7'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.7 AND 0.8) THEN '0.7-0.8'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.8 AND 0.9) THEN '0.8-0.9'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 0.9 AND 1.0) THEN '0.9-1.0'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 1.0 AND 1.1) THEN '1.0-1.1'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 1.1 AND 1.2) THEN '1.1-1.2'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 1.2 AND 1.3) THEN '1.2-1.3'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 1.3 AND 1.4) THEN '1.3-1.4'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost BETWEEN 1.4 AND 1.5) THEN '1.4-1.5'
        WHEN (one_month_cost > 0 AND one_month_revenue/one_month_cost > 1.5) THEN '1.5+'
        ELSE 'unknown'
    END AS one_month_ROI_bins,
    SUM(one_year_clicks) AS one_year_clicks,
    SUM(one_year_cost) AS one_year_cost,
    SUM(one_year_revenue) AS one_year_revenue,
    SUM(one_year_impressions) AS one_year_impressions,
    listing_id
FROM
   ouput
GROUP BY
    1,
    2,
    3,
    8;

select
listing_id, 
one_year_revenue, 
one_year_cost, 
one_year_clicks
from `etsy-data-warehouse-dev.static.de_vampire_listings`  where 
one_year_clicks >= 50 
and one_year_ROI_bins in ('0.0-0.1', '0.1-0.2', '0.2-0.3', '0.3-0.4', '0.4-0.5')
and one_month_ROI_bins  in ('0.0-0.1', '0.1-0.2', '0.2-0.3', '0.3-0.4', '0.4-0.5', 'No Spend')
