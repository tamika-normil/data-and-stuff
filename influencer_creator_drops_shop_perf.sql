-- owner: tnormil@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: Daily rollup desc performance of shops and listings featured in the influencer creator drops program

begin

DECLARE start_dt date;

SET start_dt =  '2019-12-01';

create temp table shops as 
  (select distinct cb.shop_id, cb.shop_name, user_id as seller_user_id, influencer as creator
  from `etsy-data-warehouse-prod.rollups.influencer_creator_drops_shops`cb
  left join  etsy-data-warehouse-prod.rollups.seller_basics_all a using (shop_id));

create temp table hl_shops as 
  ( select case when country_name = "United States" then "US"
    when country_name = "United Kingdom" then "GB"
    else "INTL" end as country, 
    case when seller_tier in ('power seller','top seller') then seller_tier
    else 'other' end as type, 
    seller_tier, 
    shop_id, top_category_new
    from  etsy-data-warehouse-prod.rollups.seller_basics_all
    where top_category_new in ('home_and_living', 'jewelry', 'bath_and_beauty'));

create temp table shop_gms as   
  (SELECT lv.date, shop_id, 
  sum(gms_net) as gms_net
  FROM etsy-data-warehouse-prod.transaction_mart.all_transactions lv
  join etsy-data-warehouse-prod.transaction_mart.transactions_gms tg using (transaction_id)
  left join `etsy-data-warehouse-prod.listing_mart.listing_vw` using (listing_id)
  where lv.date >= start_dt
  group by 1,2);


create temp table shop_listing_visits as 
  (SELECT distinct date(v.start_datetime) as date, l.shop_id, lv.visit_id, converted
  FROM `etsy-data-warehouse-prod.analytics.listing_views` lv 
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on lv.visit_id = v.visit_id
  left join `etsy-data-warehouse-prod.listing_mart.listing_vw` l on lv.listing_id = l.listing_id
  where lv._date >= start_dt and v._date >= start_dt);

create temp table shop_home_visits as 
  (SELECT distinct date(visit_start_date) as date,  sb.shop_id, sv.visit_id, v.converted
  FROM `etsy-data-warehouse-prod.rollups.shop_home_visit_shop_id_record` sv
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on sv.visit_id = v.visit_id
  join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sv.shop_id = cast(sb.shop_id as string)
  left join shop_listing_visits slv on sv.visit_id = slv.visit_id
  where visit_start_date >=  start_dt
  and slv.visit_id is null);

create temp table shop_visits_agg as 
  (with shop_visits as
      (SELECT * from shop_home_visits union distinct select * from shop_listing_visits)
  select date, shop_id,
  count(distinct visit_id) as visits,
  sum(converted) as converted
  from shop_visits sv
  group by 1,2);

create temp table shop_perf_agg as 
  (with keys as 
    (select distinct date,
      shop_id,
      from shop_gms
      union distinct  
      select distinct date,
      shop_id,
      from shop_visits_agg)
  select k.*,
  gms_net as shop_gms,
  visits as shop_visits,
  converted as shop_converted_visits 
  from keys k
  left join shop_gms sg using (date, shop_id)
  left join shop_visits_agg sv using (date, shop_id));

create or replace table `etsy-data-warehouse-dev.tnormil.cb_shop_perf` as
  (select a.date,  creator, sum(coalesce(shop_gms,0)) as shop_gms ,
  sum(coalesce(shop_visits,0)) as shop_visits,
  sum(coalesce(shop_converted_visits,0)) as shop_converted_visits 
  from shop_perf_agg a
  join shops b using (shop_id)
  group by 1,2);

create or replace table `etsy-data-warehouse-dev.tnormil.overall_shop_perf` as
  (select a.date, b.country, b.type, seller_tier, top_category_new, case when b.shop_id is not null then 1 else 0 end as drops_listing, shop_id,sum(coalesce(shop_gms,0)) as shop_gms ,
  sum(coalesce(shop_visits,0)) as shop_visits,
  sum(coalesce(shop_converted_visits,0)) as shop_converted_visits 
  from shop_perf_agg a
  join hl_shops b using (shop_id)
  left join shops c using (shop_id)
  group by 1,2,3,4,5,6,7);

end 
