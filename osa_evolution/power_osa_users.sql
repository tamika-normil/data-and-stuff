#top and power sellers with osa clicks in the past 3 months ordered by total number of clicks in the past 3 months
#to be eligible, sellers must have another click within 30 days of their most recent click to ensure they are consistently drivng clicks.

with sum_clicks as 
    (select shop_id, date(timestamp_seconds(click_date)) as click_date, count(*) as clicks 
    from `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks` a
    where date(timestamp_seconds(click_date)) >= date_sub(current_date, interval 3 month)
    group by 1,2),
timely_clicks as
    (select distinct shop_id, click_date, 
    row_number( ) over (partition by shop_id order by click_date desc) as rnk, 
    lag(click_date) over (partition by shop_id order by click_date asc) as previous_click_date,
    sum(clicks) over (partition by shop_id order by click_date asc) as running_sum_click
    from sum_clicks
    qualify rnk = 1
    ) 
select t.shop_id, t.click_date, t.previous_click_date, t.running_sum_click, s.seller_tier
from timely_clicks t
join etsy-data-warehouse-prod.rollups.seller_basics s using (shop_id)
where seller_tier in ("power seller","top seller")
and date_diff(click_date, previous_click_date, day) < 30
order by running_sum_click desc;
