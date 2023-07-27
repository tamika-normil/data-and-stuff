--creator drop

-- owner: tnormil@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: Daily rollup desc performance of shops and listings featured in the influencer creator drops program

begin

DECLARE start_dt date;

SET start_dt =  '2019-12-01';

create temp table shops as 
   ( select distinct shop_id, shop_name, user_id as seller_user_id, 
   case when regexp_contains(title, r'\s(Creator Drop-)\s') then trim(left(replace(title,'PRE-ORDER:',''), REGEXP_INSTR(replace(title,'PRE-ORDER:',''), r'\s(Creator Drop-)\s') - 1))
   else trim(left(replace(title,'PRE-ORDER:',''), REGEXP_INSTR(replace(title,'PRE-ORDER:',''), r'\s(Creator Drop)\s') - 1)) end as influencer
   from `etsy-data-warehouse-prod.listing_mart.listing_vw` 
   where regexp_contains(title, r'\s(Creator Drop)\s') or regexp_contains(title, r'\s(Creator Drop-)\s'));

create temp table listings as 
   ( select distinct shop_id, shop_name, user_id as seller_user_id, listing_id, title,
   case when regexp_contains(title, r'\s(Creator Drop-)\s') then trim(left(replace(title,'PRE-ORDER:',''), REGEXP_INSTR(replace(title,'PRE-ORDER:',''), r'\s(Creator Drop-)\s') - 1))
   else trim(left(replace(title,'PRE-ORDER:',''), REGEXP_INSTR(replace(title,'PRE-ORDER:',''), r'\s(Creator Drop)\s') - 1)) end as influencer
    from `etsy-data-warehouse-prod.listing_mart.listing_vw` 
    where regexp_contains(title, r'\s(Creator Drop)\s') or regexp_contains(title, r'\s(Creator Drop-)\s'));
    
create temp table shop_listing_visits as 
  (SELECT distinct date(v.start_datetime) as date, yt.shop_id, lv.visit_id,
  v.top_channel,
  v.second_channel,
  v.third_channel,
  v.utm_medium,
  v.utm_campaign,
  v.utm_content
  FROM shops yt
  join `etsy-data-warehouse-prod.analytics.listing_views` lv on yt.seller_user_id = lv.seller_user_id
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on lv.visit_id = v.visit_id
  where lv._date >= start_dt and v._date >= start_dt);

create temp table shop_home_visits as 
  (SELECT distinct date(visit_start_date) as date, yt.shop_id, sv.visit_id,
  v.top_channel,
  v.second_channel,
  v.third_channel,
  v.utm_medium,
  v.utm_campaign,
  v.utm_content
  FROM shops yt
  join `etsy-data-warehouse-prod.rollups.shop_home_visit_shop_id_record`  sv on cast(yt.shop_id as string) = sv.shop_id
  join `etsy-data-warehouse-prod.buyatt_mart.visits` v on sv.visit_id = v.visit_id
  left join shop_listing_visits slv on sv.visit_id = slv.visit_id
  where visit_start_date >=  start_dt
  and slv.visit_id is null);


create or replace table `etsy-data-warehouse-dev.rollups.influencer_creator_drops_shops` as 
(with shop_gms as   
  (SELECT lv.date, shop_id, 
  coalesce(v.top_channel,'') as top_channel,
  coalesce(v.second_channel,'') as second_channel,
  coalesce(v.third_channel,'') as third_channel,
  coalesce(v.utm_medium,'') as utm_medium,
  coalesce(v.utm_campaign,'') as utm_campaign,
  coalesce(v.utm_content,'') as utm_content,
  sum(gms_net) as gms_net
  FROM shops yt
  join `etsy-data-warehouse-prod.transaction_mart.all_transactions` lv using (seller_user_id)
  join `etsy-data-warehouse-prod.transaction_mart.transactions_gms`  tg using (transaction_id)
  join `etsy-data-warehouse-prod.transaction_mart.transactions_visits`  tv using (transaction_id)
  join `etsy-data-warehouse-prod.buyatt_mart.visits`  v on tv.visit_id = v.visit_id
  where lv.date >= start_dt
  and _date >= start_dt
  group by 1,2,3,4,5,6,7,8),
shop_visits as
  (SELECT * from shop_home_visits union distinct select * from shop_listing_visits),
shop_visits_agg as 
  (select date, shop_id,
  coalesce(top_channel,'') as top_channel,
  coalesce(second_channel,'') as second_channel,
  coalesce(third_channel,'') as third_channel,
  coalesce(utm_medium,'') as utm_medium,
  coalesce(utm_campaign,'') as utm_campaign,
  coalesce(utm_content,'') as utm_content,
  count(distinct visit_id) as visits
  from shop_visits sv
  group by 1,2,3,4,5,6,7,8),
shop_perf as
  (SELECT date(receipt_timestamp) as date, sv.shop_id, 
  top_channel,
  second_channel,
  third_channel,
  utm_medium,
  utm_campaign,
  utm_content,  
  sum(b.external_source_decay_all * b.gms) AS attr_gms,
  sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,
  sum(b.external_source_decay_all) AS attr_receipt,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'new' as int64))) as attributed_new_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_receipts,
  sum(b.external_source_decay_all*b.gms*(cast(b.buyer_type= 'new' as int64))) as attributed_new_gms,
  sum(b.external_source_decay_all*b.gms*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_gms,
  sum(b.external_source_decay_all*b.gms*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_gms,
  sum(b.external_source_decay_all*c.attr_rev*(cast(b.buyer_type= 'new' as int64))) as attributed_new_rev,
  sum(b.external_source_decay_all*c.attr_rev*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_rev,
  sum(b.external_source_decay_all*c.attr_rev*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_rev, 
  sum(b.last_click_all * b.gms) AS insess_gms,
  sum(b.last_click_all * c.attr_rev) AS insess_rev,
  sum(b.last_click_all) AS insess_receipts,
  sum(b.last_click_all*(cast(b.buyer_type= 'new' as int64))) as insess_new_receipts,
  sum(b.last_click_all*(cast(b.buyer_type= 'lapsed' as int64))) as insess_lapsed_receipts,
  sum(b.last_click_all*(cast(b.buyer_type= 'existing' as int64))) as insess_existing_receipts,
  sum(b.last_click_all*b.gms*(cast(b.buyer_type= 'new' as int64))) as insess_new_gms,
  sum(b.last_click_all*b.gms*(cast(b.buyer_type= 'lapsed' as int64))) as insess_lapsed_gms,
  sum(b.last_click_all*b.gms*(cast(b.buyer_type= 'existing' as int64))) as insess_existing_gms,
  sum(b.last_click_all*c.attr_rev*(cast(b.buyer_type= 'new' as int64))) as insess_new_rev,
  sum(b.last_click_all*c.attr_rev*(cast(b.buyer_type= 'lapsed' as int64))) as insess_lapsed_rev,
  sum(b.last_click_all*c.attr_rev*(cast(b.buyer_type= 'existing' as int64))) as insess_existing_rev,   
  FROM shop_visits sv
  join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` b ON sv.visit_id = b.o_visit_id
  join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c ON b.receipt_id = c.receipt_id
  group by 1,2,3,4,5,6,7,8),
keys as 
  (select distinct date,
     shop_id,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_content,
    from shop_gms
    union distinct  
    select distinct date,
     shop_id,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_content,
    from shop_visits_agg   
    union distinct 
    select distinct date,
      shop_id,
      top_channel,
      second_channel,
      third_channel,
      utm_campaign,
      utm_medium, 
      utm_content,
    from shop_perf)
select k.*, shop_name, influencer,
gms_net as shop_gms,
visits as shop_visits,
attr_gms,
attr_rev,
attr_receipt,
attributed_new_receipts,
attributed_lapsed_receipts,
attributed_existing_receipts,
attributed_new_gms,
attributed_lapsed_gms,
attributed_existing_gms,
attributed_new_rev,
attributed_lapsed_rev,
attributed_existing_rev, 
insess_gms,
insess_rev,
insess_receipts,
insess_new_receipts,
insess_lapsed_receipts,
insess_existing_receipts,
insess_new_gms,
insess_lapsed_gms,
insess_existing_gms,
insess_new_rev,
insess_lapsed_rev,
insess_existing_rev, 
from keys k
left join shops s using (shop_id)
left join shop_gms sg using (date, shop_id, top_channel, second_channel, third_channel, utm_medium, utm_campaign, utm_content)
left join shop_visits_agg sv using (date, shop_id, top_channel, second_channel, third_channel, utm_medium, utm_campaign, utm_content)
left join shop_perf sp using (date, shop_id, top_channel, second_channel, third_channel, utm_medium, utm_campaign, utm_content));


create or replace table `etsy-data-warehouse-dev.rollups.influencer_creator_drops_shops_buyer_type` as 
(with shop_visits as
  (SELECT * from shop_home_visits union distinct select * from shop_listing_visits),
shop_perf as
  (SELECT date(receipt_timestamp) as date, sv.shop_id, 
  top_channel,
  second_channel,
  third_channel,
  utm_medium,
  utm_campaign,
  utm_content,  
      CASE
        WHEN c.buyer_type = 'new_buyer' THEN 'new_buyer'
        WHEN c.purchase_day_number = 2
         AND c.buyer_type <> 'reactivated_buyer' THEN '2x_buyer'
        WHEN c.purchase_day_number = 3
         AND c.buyer_type <> 'reactivated_buyer' THEN '3x_buyer'
        WHEN c.purchase_day_number >= 4 and c.purchase_day_number<= 9
         AND c.buyer_type <> 'reactivated_buyer' THEN '4_to_9x_buyer'
        WHEN c.purchase_day_number >= 10
         AND c.buyer_type <> 'reactivated_buyer' THEN '10plus_buyer'
        WHEN c.buyer_type = 'reactivated_buyer' THEN 'reactivated_buyer'
        ELSE 'other'
      END AS buyer_type,
  sum(b.external_source_decay_all * b.gms) AS attr_gms,
  sum(b.external_source_decay_all * c.attr_rev) AS attr_rev,
  sum(b.external_source_decay_all) AS attr_receipt,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'new' as int64))) as attributed_new_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_receipts,
  sum(b.external_source_decay_all*b.gms*(cast(b.buyer_type= 'new' as int64))) as attributed_new_gms,
  sum(b.external_source_decay_all*b.gms*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_gms,
  sum(b.external_source_decay_all*b.gms*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_gms,
  sum(b.external_source_decay_all*c.attr_rev*(cast(b.buyer_type= 'new' as int64))) as attributed_new_rev,
  sum(b.external_source_decay_all*c.attr_rev*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_rev,
  sum(b.external_source_decay_all*c.attr_rev*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_rev, 
  sum(b.last_click_all * b.gms) AS insess_gms,
  sum(b.last_click_all * c.attr_rev) AS insess_rev,
  sum(b.last_click_all) AS insess_receipts,
  sum(b.last_click_all*(cast(b.buyer_type= 'new' as int64))) as insess_new_receipts,
  sum(b.last_click_all*(cast(b.buyer_type= 'lapsed' as int64))) as insess_lapsed_receipts,
  sum(b.last_click_all*(cast(b.buyer_type= 'existing' as int64))) as insess_existing_receipts,
  sum(b.last_click_all*b.gms*(cast(b.buyer_type= 'new' as int64))) as insess_new_gms,
  sum(b.last_click_all*b.gms*(cast(b.buyer_type= 'lapsed' as int64))) as insess_lapsed_gms,
  sum(b.last_click_all*b.gms*(cast(b.buyer_type= 'existing' as int64))) as insess_existing_gms,
  sum(b.last_click_all*c.attr_rev*(cast(b.buyer_type= 'new' as int64))) as insess_new_rev,
  sum(b.last_click_all*c.attr_rev*(cast(b.buyer_type= 'lapsed' as int64))) as insess_lapsed_rev,
  sum(b.last_click_all*c.attr_rev*(cast(b.buyer_type= 'existing' as int64))) as insess_existing_rev,   
  FROM shop_visits sv
  join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` b ON sv.visit_id = b.o_visit_id
  join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c ON b.receipt_id = c.receipt_id
  group by 1,2,3,4,5,6,7,8,9)
select * 
from shop_perf);

end

select date(date_trunc(visit_date, month)) as month,reporting_channel_group, buyer_type, sum(attr_receipts) as attr_receipts,
sum(attr_gms) as attr_gms
from `etsy-data-warehouse-prod.buyatt_rollups.buyer_growth_metrics` b
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd using (utm_campaign,				
utm_medium,			
top_channel,		
second_channel,				
third_channel)
where concat(ifnull(utm_campaign,''),ifnull(utm_content,'')) like '%creatordrop_leenasnoubar%'
or concat(ifnull(utm_campaign,''),ifnull(utm_content,'')) like '%1335165%'
group by 1,2,3
union all
select date(date_trunc(date, month)) as month,reporting_channel_group,buyer_type, sum(attr_receipt) as attr_receipts,
sum(attr_gms) as attr_gms
from `etsy-data-warehouse-dev.rollups.influencer_creator_drops_shops_buyer_type`
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd using (utm_campaign,				
utm_medium,			
top_channel,		
second_channel,				
third_channel)
where concat(ifnull(utm_campaign,''),ifnull(utm_content,'')) not like '%creatordrop_leenasnoubar%'
and concat(ifnull(utm_campaign,''),ifnull(utm_content,'')) not like '%1335165%'
and date >= '2023-06-01'
group by 1,2,3
