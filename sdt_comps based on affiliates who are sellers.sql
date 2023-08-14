#for affiliates who are sellers, what is the average number of visits. how many are att to affiliate and non paid social traffic.

begin

DECLARE start_dt date;

SET start_dt =  '2019-12-01';

create temp table select_sellers as 
(select distinct user_id as seller_user_id
from `etsy-data-warehouse-prod.etsy_shard.affiliate_users`);


create temp table shop_listing_visits_temp as 
(with shop_listing_visits_tb as 
  (SELECT date(v.start_datetime) as date, yt.seller_user_id, replace(replace(reporting_channel_group,' ',''),'-','') as reporting_channel_group ,count(distinct lv.visit_id) as shop_listing_visits
  FROM select_sellers yt
  join `etsy-data-warehouse-prod.analytics.listing_views` lv on yt.seller_user_id = lv.seller_user_id
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on lv.visit_id = v.visit_id
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using ( top_channel, second_channel, third_channel, utm_medium, utm_campaign)
  where lv._date >= start_dt and v._date >= start_dt
  and reporting_channel_group in ( 'Affiliates', 'Dark', 'NonPaid Social') 
  group by 1, 2, 3)
select *
from shop_listing_visits_tb
  PIVOT(sum(shop_listing_visits )  as shop_listing_visits FOR reporting_channel_group in ( 'Affiliates', 'Dark', 'NonPaidSocial') ));

create temp table shop_home_visits_temp as 
(with shop_home_visits_tb as
(SELEct date(visit_start_date) as date, yt.seller_user_id, replace(replace(reporting_channel_group,' ',''),'-','') as reporting_channel_group ,count(distinct sv.visit_id) as shop_home_visits 
  FROM select_sellers yt
  join `etsy-data-warehouse-prod.rollups.shop_home_visit_shop_id_record`  sv on cast(yt.seller_user_id as string) = sv.seller_user_id
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on sv.visit_id = v.visit_id
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using ( top_channel, second_channel, third_channel, utm_medium, utm_campaign)
  where visit_start_date >=  start_dt
  and reporting_channel_group in ( 'Affiliates', 'Dark', 'Non-Paid Social') 
  group by 1,2,3)
select *
from shop_home_visits_tb
  PIVOT(sum( shop_home_visits )  shop_home_visits FOR reporting_channel_group in ( 'Affiliates', 'Dark', 'NonPaidSocial') ));


select date(_date) as date, seller_tier, sum(sm.shop_home_visits) as shop_home_visits, sum(sm.view_listing_visits) as view_listing_visits,
count(distinct coalesce(slv.seller_user_id, shv.seller_user_id)) as sellers,

sum(coalesce(shop_listing_visits_Affiliates,0)) as shop_listing_visits_Affiliates,
sum(coalesce(shop_listing_visits_Dark,0)) as shop_listing_visits_Dark,
sum(coalesce(shop_listing_visits_NonPaidSocial,0)) as shop_listing_visits_NonPaidSocial,

sum(coalesce(shop_home_visits_Affiliates,0)) as shop_home_visits_Affiliates,
sum(coalesce(shop_home_visits_Dark,0)) as shop_home_visits_Dark,
sum(coalesce(shop_home_visits_NonPaidSocial,0)) as shop_home_visits_NonPaidSocial,
from `etsy-data-warehouse-prod.analytics.shop_metrics` sm
join select_sellers ss on sm.user_id = ss.seller_user_id
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on  ss.seller_user_id = sb.user_id
left join shop_listing_visits_temp slv on ss.seller_user_id = slv.seller_user_id and sm._date = slv.date
left join shop_home_visits_temp shv on  ss.seller_user_id = shv.seller_user_id and sm._date = shv.date
where sm._date >= start_dt 
group by 1,2;

end
