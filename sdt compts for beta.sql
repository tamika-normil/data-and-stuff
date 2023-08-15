begin

DECLARE start_dt date;

SET start_dt =  '2019-12-01';

create temp table select_sellers as 
(SELECT distinct user_id as seller_user_id
FROM `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` 
where sdt_credit_usd > 0);

create temp table shop_listing_visits_temp as 
(SELECT distinct date(v.start_datetime) as date, yt.seller_user_id, lv.visit_id
  FROM select_sellers yt
  join `etsy-data-warehouse-prod.analytics.listing_views` lv on yt.seller_user_id = lv.seller_user_id
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on lv.visit_id = v.visit_id
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using ( top_channel, second_channel, third_channel, utm_medium, utm_campaign)
  where lv._date >= start_dt and v._date >= start_dt);


create temp table shop_listing_visits_temp_agg as 
(SELECT date, seller_user_id as user_id, count(distinct visit_id) as shop_listing_visits
  FROM shop_listing_visits_temp
  group by 1, 2);

create temp table shop_home_visits_temp_agg as 
(SELEct date(visit_start_date) as date, yt.seller_user_id as user_id,count(distinct sv.visit_id) as shop_home_visits 
  FROM select_sellers yt
  join `etsy-data-warehouse-prod.rollups.shop_home_visit_shop_id_record`  sv on cast(yt.seller_user_id as string) = sv.seller_user_id
  left join shop_listing_visits_temp slv on sv.visit_id = slv.visit_id
  where visit_start_date >=  start_dt
  and slv.visit_id is null
  group by 1,2);
  

create temp table sdt_visits as
(select sb.user_id, date(timestamp_seconds(ac.click_date)) as date, count(distinct visit_id) as visits
from  `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks` ac
join etsy-data-warehouse-prod.rollups.osa_click_to_visit_join osa_v using (click_id)
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb using (shop_id)
where channel = 8
group by 1,2  );


select date(_date) as date, seller_tier, sum(sm.shop_home_visits) as shop_home_visits, sum(sm.view_listing_visits) as view_listing_visits,
sum(shv.shop_home_visits) as shop_home_visits_unique, sum(slv.shop_listing_visits) as shop_listing_visits_unique,
count(distinct sm.user_id) as sellers,
sum(coalesce(visits,0)) as sdt_visits,
from `etsy-data-warehouse-prod.analytics.shop_metrics` sm
join select_sellers ss on sm.user_id = ss.seller_user_id
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on  ss.seller_user_id = sb.user_id
left join sdt_visits sdt on sm._date = sdt.date and sm.user_id = sdt.user_id
left join shop_listing_visits_temp_agg slv on sm._date = slv.date and sm.user_id = slv.user_id
left join  shop_home_visits_temp_agg shv on sm._date = shv.date and sm.user_id = shv.user_id
where sm._date >= start_dt 
group by 1,2;
