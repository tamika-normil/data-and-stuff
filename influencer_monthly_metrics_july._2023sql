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

-- collab

-- owner: tnormil@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- dependencies: `etsy-data-warehouse-prod.etsy_index.finds_page`, `etsy-data-warehouse-prod.buyatt_mart.visits`, `etsy-data-warehouse-prod.buyer_growth.editors_picks_visit_metrics`, `etsy-data-warehouse-prod.etsy_shard.finds_page_module_data`,`etsy-data-warehouse-prod.weblog.events`, `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser`, `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv`
-- description: This rollup supports a Looker dashboard that measures the performance of all Influnecer programs. This code calculates the attributed and in session GMS from influencer marketing, listing engagement, page views, and connected profile views. 
-- access: etsy-data-warehouse-prod.rollups.influencer_favorites_overview= group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com
-- access: etsy-data-warehouse-prod.rollups.influencer_favorites_overview_toplevel= group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com

BEGIN

DECLARE program_start_date date;



CREATE TABLE IF NOT EXISTS `etsy-data-warehouse-dev.rollups.influencer_favorites_overview`
(
  ep_page_title STRING,
  ep_publish_date DATE,
  slug STRING,
  type STRING,
  run_date INT64,
  platform_device STRING,
  third_channel STRING,
  second_channel STRING,
  top_channel STRING,
  channel_group STRING,
  utm_campaign STRING,
  utm_custom2 STRING,
  utm_medium STRING,
  utm_source STRING,
  utm_content STRING,
  marketing_region STRING,
  key_market STRING,
  visit_market STRING,
  buyer_type_detail STRING,
  target_gender STRING,
  visits INT64,
  insession_gms NUMERIC,
  insession_gms_gross NUMERIC,
  insession_conversions INT64,
  insession_gms_direct NUMERIC,
  insession_gms_gross_direct NUMERIC,
  insession_conversions_direct INT64,
  attributed_gms FLOAT64,
  attributed_attr_rev FLOAT64,
  attributed_receipts FLOAT64,
  attributed_new_receipts FLOAT64,
  attributed_lapsed_receipts FLOAT64,
  attributed_existing_receipts FLOAT64,
  attributed_new_gms FLOAT64,
  attributed_lapsed_gms FLOAT64,
  attributed_existing_gms FLOAT64,
  attributed_new_attr_rev FLOAT64,
  attributed_lapsed_attr_rev FLOAT64,
  attributed_existing_attr_rev FLOAT64,
  attributed_gms_direct FLOAT64,
  attributed_attr_rev_direct FLOAT64,
  attributed_receipts_direct FLOAT64,
  attributed_new_receipts_direct FLOAT64,
  attributed_lapsed_receipts_direct FLOAT64,
  attributed_existing_receipts_direct FLOAT64,
  attributed_new_gms_direct FLOAT64,
  attributed_lapsed_gms_direct FLOAT64,
  attributed_existing_gms_direct FLOAT64,
  attributed_new_attr_rev_direct FLOAT64,
  attributed_lapsed_attr_rev_direct FLOAT64,
  attributed_existing_attr_rev_direct FLOAT64,
  insession_new_conversions INT64,
  insession_lapsed_conversions INT64,
  insession_existing_conversions INT64,
  insession_new_gms NUMERIC,
  insession_lapsed_gms NUMERIC,
  insession_existing_gms NUMERIC,
  insession_new_conversions_direct INT64,
  insession_lapsed_conversions_direct INT64,
  insession_existing_conversions_direct INT64,
  insession_new_gms_direct NUMERIC,
  insession_lapsed_gms_direct NUMERIC,
  insession_existing_gms_direct NUMERIC
);


set program_start_date = (SELECT COALESCE(date_sub(cast(timestamp_seconds(max(run_date)) as date), interval 30 day), DATE('2018-11-01')) FROM `etsy-data-warehouse-dev.rollups.influencer_favorites_overview`);


create temp table collection_ep as
(with ep as (select REGEXP_REPLACE(lower(slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$|-in$', "")  as ep_page_title, min(cast(timestamp_seconds(publish_date) as date)) as ep_publish_date
from `etsy-data-warehouse-prod.etsy_index.finds_page`
where (title in ('Whoopi Loves the Holidays', "Kelly Rowland’s favorite finds", "Dan Levy’s favorite finds", 
'Favorites from Jessie and Lennie Ware') 
or slug like 'jessie-and-lennie%'
or slug like 'iris-apfel%'
or merch_page_type = 'Partnerships & Collaborations'
or lower(title) like '%x etsy%'
or lower(subtitle) like '%x etsy%' 
or lower(subtitle) like '%collab%' 
or lower(subtitle) like '%kollab%')
and publish_date <> 0
-- this filter will change to merch_page_type in (collections, favorites, the etsy edit)
group by 1)
select distinct ep.*, fp.slug, fp.finds_page_id, subtitle, seo_title,
case when lower(subtitle) like '%x etsy%' or lower(subtitle) like '%collab%' or lower(subtitle) like '%kollab%' then 'collection'
when fp.slug like '%etsyedit%' then 'etsy edit' else 'favorites' end as program
from ep
join `etsy-data-warehouse-prod.etsy_index.finds_page`fp on ep.ep_page_title  = REGEXP_REPLACE(lower(fp.slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$|-in$', "") 
where publish_date <> 0
and ep.ep_page_title = 'johnlegend');

create temp table collection_listings as
    (select distinct 
    p.slug
    ,p.ep_page_title
    ,p.finds_page_id
    ,p.ep_publish_date
    ,p.subtitle
    ,l.listing_id
    from collection_ep p
    join etsy-data-warehouse-prod.etsy_shard.finds_listings l using (finds_page_id) );
    
create temp table collection_visits as 
(select ep_page_title, 
ep_publish_date, 
slug,
'influencer affiliate marketing' as type,
v.*,
1 as get_gms, 
1 as get_valid_gms
from `etsy-data-warehouse-prod.buyatt_mart.visits` v 
join `etsy-data-warehouse-prod.static.influencer_awin_publishers` c on v.utm_content = cast(c.utm_content as string)
join (select distinct ep_page_title, ep_publish_date, slug from collection_ep) cl on lower(cl.slug ) like '%' || lower(c.page_title) || '%' and REGEXP_CONTAINS(lower(cl.slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$|-in$') is false
where v._date >= program_start_date);
-- will update data source to static schema once bizdata support is available

insert into collection_visits
(select cl.ep_page_title, 
cl.ep_publish_date, 
cl.slug,
'influencer marketing' as type,
v.*,
1 as get_gms, 
1 as get_valid_gms
from `etsy-data-warehouse-prod.buyatt_mart.visits` v 
join `etsy-data-warehouse-prod.static.influencer_utm` c on lower(v.utm_campaign) like '%' || lower(c.utm_code) || '%'
join (select distinct ep_page_title, ep_publish_date, slug from collection_ep) cl on lower(cl.slug ) like '%' || lower(c.page_title) || '%' and REGEXP_CONTAINS(lower(cl.slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$|-in$') is false
left join collection_visits cv using (ep_page_title,visit_id)
where v._date >= program_start_date and cv.visit_id is null);
-- will update data source to static schema once bizdata support is available
    
create temp table collection_listings_engagement as (with 
view_listings as 
    (select distinct list.ep_page_title,
    list.ep_publish_date,
    list.slug, 
    list.listing_id, 
    v.visit_id, 
    timestamp_MILLIS(v.epoch_ms) as event_datetime, 
    case when lower(subtitle) not like '%x etsy%' and lower(subtitle) not like '%collab%' and lower(subtitle) not like '%kollab%' and ep.visit_id is null then 0
    when lower(subtitle) not like '%x etsy%' and lower(subtitle) not like '%collab%' and lower(subtitle) not like '%kollab%' and ep.visit_id is not null then 1  
    else 1 end as valid,
    from collection_listings list
    join etsy-data-warehouse-prod.analytics.listing_views v on list.listing_id = v.listing_id
    left join etsy-data-warehouse-prod.buyer_growth.editors_picks_event_metrics ep on v.visit_id = ep.visit_id and list.slug = ep.slug and ep._date >= program_start_date
    where cast(timestamp_MILLIS(epoch_ms) as date) >= ep_publish_date 
    and v._date >= program_start_date),
rank_listings as 
    (select *, row_number() OVER (PARTITION BY visit_id ORDER BY event_datetime ASC) AS row_number
    from view_listings
    where valid = 1)
select *
from rank_listings
where row_number = 1) ;    

insert into collection_visits (with
identify_direct_purchases as
    (select l.visit_id, case when att.listing_id is not null then 1 else 0 end as direct,
    case when event_datetime < r.creation_tsz then 1 else 0 end as viewed_before_purchase
    from  collection_listings_engagement l
    join collection_listings cl on l.slug = cl.slug
    join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv on l.visit_id = tv.visit_id
    join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on tv.receipt_id = r.receipt_id
    left join `etsy-data-warehouse-prod.transaction_mart.all_transactions` att on tv.transaction_id = att.transaction_id and cl.listing_id = att.listing_id)
select distinct l.ep_page_title, 
l.ep_publish_date, 
l.slug,
'listing engagement' as type,
v.*,
case when viewed_before_purchase = 1 then 1 else 0 end as get_gms, 
case when viewed_before_purchase = 1 and direct = 1 then 1 else 0 end as get_valid_gms
from `etsy-data-warehouse-prod.buyatt_mart.visits` v 
join  collection_listings_engagement l using (visit_id)
left join  collection_visits cv using (ep_page_title,visit_id)
left join (select visit_id, max(direct) as direct, max(viewed_before_purchase) as viewed_before_purchase from identify_direct_purchases group by 1) d on l.visit_id = d.visit_id
where v._date >= program_start_date and cv.visit_id is null);

insert into  collection_visits
(with page_views as 
(select ep_page_title, 
ep_publish_date, 
ep.slug, 
visit_id,
case when v.gms = 0 then 0 else 1 end as get_gms,
case when v.valid_gms = 0 then 0 else 1 end as get_valid_gms
from collection_ep ep
join `etsy-data-warehouse-prod.buyer_growth.editors_picks_visit_metrics` v on v.attributed_slug = ep.slug
where _date >= program_start_date)
select distinct pv.ep_page_title,
  pv.ep_publish_date,
  pv.slug,
  'page views' as type,
  v.*,
  pv.get_gms,
  pv.get_valid_gms
  from page_views pv
  inner join `etsy-data-warehouse-prod.buyatt_mart.visits` v using (visit_id)
  left join  collection_visits cv using (ep_page_title,visit_id)
  where v._date >= program_start_date and cv.visit_id is null) ;

-- Commented out due to poor performance 
/*
insert into collection_visits
(with find_profile as 
(select distinct ep.*, 
SUBSTR(value, REGEXP_INSTR(value, 'people/')+length('people/'), (REGEXP_INSTR(value, "[^A-Za-z0-9]",REGEXP_INSTR(value, 'people/')+length('people/'))) - (REGEXP_INSTR(value, 'people/')+length('people/')) ) as profile_user_id
from `etsy-data-warehouse-prod.etsy_shard.finds_page_module_data` fpm  
join collection_ep ep using (finds_page_id)
where name = 'url' and lower(value) like '%people%'),
profile_views as (SELECT * ,
case when (REGEXP_INSTR(url, "[^A-Za-z0-9]",REGEXP_INSTR(url, 'people/')+length('people/')))  = 0 then
SUBSTR(url,REGEXP_INSTR(url, 'people/')+length('people/'), (length(url) - (REGEXP_INSTR(url, 'people/')+length('people/'))+1))
else SUBSTR(url, REGEXP_INSTR(url, 'people/')+length('people/'), (REGEXP_INSTR(url, "[^A-Za-z0-9]",REGEXP_INSTR(url, 'people/')+length('people/'))) - (REGEXP_INSTR(url, 'people/')+length('people/')) ) end as profile_user_id,
row_number() over (partition by visit_id order by epoch_ms asc) as rank
FROM `etsy-data-warehouse-prod.weblog.events` 
WHERE _date >= program_start_date and event_type = 'view_profile' and url like '%people%')
select fp.ep_page_title, 
  fp.ep_publish_date, 
  'profile views' as type,
  v.*,
  1 as get_gms, 
  1 as get_valid_gms
  from find_profile fp 
  inner join profile_views pv using (profile_user_id)
  inner join `etsy-data-warehouse-prod.buyatt_mart.visits`  v using (visit_id)
  left join collection_visits cv using (ep_page_title,visit_id)
where rank = 1 and cv.visit_id is null);
*/


create temp table ranks as (
with ranks AS (
    SELECT
        a_1.buy_visit_id,
        a_1.buyer_type,
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
      END AS buyer_type_detail,
      b.target_gender,
        row_number() OVER (PARTITION BY a_1.buy_visit_id ORDER BY a_1.receipt_timestamp DESC, c.receipt_id DESC) AS row_number
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS a_1
        join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c ON a_1.receipt_id = c.receipt_id
        LEFT JOIN  `etsy-data-warehouse-prod.rollups.buyer_basics` b on c.mapped_user_id = b.mapped_user_id
  )
 SELECT
      buy_visit_id,
      buyer_type,
      buyer_type_detail,
      target_gender
    FROM ranks 
    WHERE row_number = 1) ;


create temp table collection_attribution_gms as (
select e.ep_page_title,
  e.ep_publish_date,
  e.slug,
  e.visit_id,
  --v.receipt_id,
  --timestamp_seconds(v.buy_date) as transaction_date,
  r.buyer_type,
  r.buyer_type_detail,
  r.target_gender,
  sum(external_source_decay_all) as external_source_decay_all,
  sum(v.external_source_decay_all*v.gms) as attr_gms,
  sum(v.external_source_decay_all*av.attr_rev) as attr_rev,
  sum(last_click_all*v.gms) as insess_gms
from  
collection_visits e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` v on e.visit_id = v.o_visit_id
join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` av using (receipt_id)
join ranks r on v.buy_visit_id = r.buy_visit_id
where get_gms = 1
group by 1,2,3,4,5,6,7);

create temp table collection_attribution_valid_gms as (
select e.ep_page_title,
  e.ep_publish_date,
  e.slug,
  e.visit_id,
  --v.receipt_id,
  --timestamp_seconds(v.buy_date) as transaction_date,
  r.buyer_type,
  r.buyer_type_detail,
  r.target_gender,
  sum(external_source_decay_all) as external_source_decay_all,
  sum(v.external_source_decay_all*v.gms) as attr_gms,
  sum(v.external_source_decay_all*av.attr_rev) as attr_rev,
  sum(last_click_all*v.gms) as insess_gms
from 
collection_visits e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` v on e.visit_id = v.o_visit_id
join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` av using (receipt_id)
join ranks r on v.buy_visit_id = r.buy_visit_id
where get_valid_gms = 1
group by 1,2,3,4,5,6,7);

create temp table collection_insess_gms as (
select e.ep_page_title,
  e.ep_publish_date,
  e.slug,
  e.visit_id,
  --gt.receipt_id,
  --gt.date as transaction_date,
  av.buyer_type,
  av.buyer_type_detail,
  av.target_gender,
  sum(gt.gms_net) as insess_gms,
  sum(gt.gms_gross) as insess_gms_gross
from  collection_visits e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv using (visit_id)
join `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` gt using (transaction_id)
join ranks av on tv.visit_id = av.buy_visit_id
where get_gms = 1
group by 1,2,3,4,5,6,7);

create temp table collection_insess_valid_gms as (
select e.ep_page_title,
  e.ep_publish_date,
  e.slug,
  e.visit_id,
  --gt.receipt_id,
  --gt.date as transaction_date,
  av.buyer_type,
  av.buyer_type_detail,
  av.target_gender,
  sum(gt.gms_net) as insess_gms,
  sum(gt.gms_gross) as insess_gms_gross
from  collection_visits e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv using (visit_id)
join `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` gt using (transaction_id)
join ranks av on tv.visit_id = av.buy_visit_id
where get_valid_gms = 1
group by 1,2,3,4,5,6,7);

delete from `etsy-data-warehouse-dev.rollups.influencer_favorites_overview`
where CAST(timestamp_seconds(run_date) AS DATE) >= program_start_date;


insert into `etsy-data-warehouse-dev.rollups.influencer_favorites_overview` (
  SELECT  
  a.ep_page_title,
  a.ep_publish_date,
  a.slug,
  type,
  run_date,     
  platform_device,
  third_channel,
  second_channel,
  top_channel,
  channel_group,
  utm_campaign,
  utm_custom2,
  utm_medium,
  utm_source,
  utm_content,
  marketing_region,
  key_market,
  visit_market,
  coalesce(b.buyer_type_detail, c.buyer_type_detail,d.buyer_type_detail,e.buyer_type_detail) as buyer_type_detail,
  coalesce(b.target_gender, c.target_gender,d.target_gender,e.target_gender) as target_gender,
  count(distinct a.visit_id) as visits,
  sum(d.insess_gms) as insession_gms,
  sum(d.insess_gms_gross) as insession_gms_gross,
  count(distinct d.visit_id) as insession_conversions,
  sum(e.insess_gms) as insession_gms_direct,
  sum(e.insess_gms_gross) as insession_gms_gross_direct,
  count(distinct e.visit_id) as insession_conversions_direct,
  sum(b.attr_gms) as attributed_gms,
  sum(b.attr_rev) as attributed_attr_rev,
  sum(b.external_source_decay_all) as attributed_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'new' as int64))) as attributed_new_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_receipts,
  sum(b.external_source_decay_all*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_receipts,
  sum(b.attr_gms*(cast(b.buyer_type= 'new' as int64))) as attributed_new_gms,
  sum(b.attr_gms*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_gms,
  sum(b.attr_gms*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_gms,
  sum(b.attr_rev*(cast(b.buyer_type= 'new' as int64))) as attributed_new_attr_rev,
  sum(b.attr_rev*(cast(b.buyer_type= 'lapsed' as int64))) as attributed_lapsed_attr_rev,
  sum(b.attr_rev*(cast(b.buyer_type= 'existing' as int64))) as attributed_existing_attr_rev,
  sum(c.attr_gms) as attributed_gms_direct,
  sum(c.attr_rev) as attributed_attr_rev_direct,
  sum(c.external_source_decay_all) as attributed_receipts_direct,
  sum(c.external_source_decay_all*(cast(c.buyer_type= 'new' as int64))) as attributed_new_receipts_direct,
  sum(c.external_source_decay_all*(cast(c.buyer_type= 'lapsed' as int64))) as attributed_lapsed_receipts_direct,
  sum(c.external_source_decay_all*(cast(c.buyer_type= 'existing' as int64))) as attributed_existing_receipts_direct,
  sum(c.attr_gms*(cast(c.buyer_type= 'new' as int64))) as attributed_new_gms_direct,
  sum(c.attr_gms*(cast(c.buyer_type= 'lapsed' as int64))) as attributed_lapsed_gms_direct,
  sum(c.attr_gms*(cast(c.buyer_type= 'existing' as int64))) as attributed_existing_gms_direct,
  sum(c.attr_rev*(cast(c.buyer_type= 'new' as int64))) as attributed_new_attr_rev_direct,
  sum(c.attr_rev*(cast(c.buyer_type= 'lapsed' as int64))) as attributed_lapsed_attr_rev_direct,
  sum(c.attr_rev*(cast(c.buyer_type= 'existing' as int64))) as attributed_existing_attr_rev_direct,
  count(distinct case when d.buyer_type= 'new' then d.visit_id end) as insession_new_conversions,
  count(distinct case when d.buyer_type= 'lapsed' then d.visit_id end) as insession_lapsed_conversions,
  count(distinct case when d.buyer_type= 'existing' then d.visit_id end) as insession_existing_conversions,
  sum(d.insess_gms*(cast(d.buyer_type= 'new' as int64))) as insession_new_gms,
  sum(d.insess_gms*(cast(d.buyer_type= 'lapsed' as int64))) as insession_lapsed_gms,
  sum(d.insess_gms*(cast(d.buyer_type= 'existing' as int64))) as insession_existing_gms,
  count(distinct case when e.buyer_type= 'new' then e.visit_id end) as insession_new_conversions_direct,
  count(distinct case when e.buyer_type= 'lapsed' then e.visit_id end) as insession_lapsed_conversions_direct,
  count(distinct case when e.buyer_type= 'existing' then e.visit_id end) as insession_existing_conversions_direct,
  sum(d.insess_gms*(cast(e.buyer_type= 'new' as int64))) as insession_new_gms_direct,
  sum(d.insess_gms*(cast(e.buyer_type= 'lapsed' as int64))) as insession_lapsed_gms_direct,
  sum(d.insess_gms*(cast(e.buyer_type= 'existing' as int64))) as insession_existing_gms_direct,
  from collection_visits  a
  left join collection_attribution_gms b on a.ep_page_title = b.ep_page_title and a.visit_id = b.visit_id and a.slug = b.slug
  left join collection_attribution_valid_gms c on a.ep_page_title = c.ep_page_title and a.visit_id = c.visit_id and a.slug = c.slug
  left join collection_insess_gms d on a.ep_page_title = d.ep_page_title and a.visit_id = d.visit_id and a.slug = d.slug  
  left join collection_insess_valid_gms e on a.ep_page_title = e.ep_page_title and a.visit_id = e.visit_id and a.slug = e.slug
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
  
create or replace table `etsy-data-warehouse-dev.rollups.influencer_favorites_overview_toplevel` as
(with get_program as (SELECT ep_page_title, program, row_number() OVER (PARTITION BY ep_page_title ORDER BY program ASC) as rank
from  collection_ep)
select a.ep_page_title,
ep_publish_date,
slug,
a.ep_page_title as slug_adjusted,
type,
run_date,
platform_device,
third_channel,
second_channel,
top_channel,
channel_group,
utm_campaign,
utm_custom2,
utm_medium,
utm_source,
utm_content,
marketing_region,
key_market,
visit_market,
buyer_type_detail,
target_gender,
visits,
attributed_gms_direct as gms,
attributed_attr_rev_direct as rev,
attributed_receipts_direct as conversions,
attributed_lapsed_receipts_direct as lapsed_conversions,
attributed_new_receipts_direct as new_conversions,
attributed_existing_receipts_direct as existing_conversions,
attributed_new_gms_direct as new_gms,
attributed_lapsed_gms_direct as lapsed_gms,
attributed_existing_gms_direct as existing_gms,
attributed_new_attr_rev_direct as new_rev,
attributed_lapsed_attr_rev_direct as lapsed_rev,
attributed_existing_attr_rev_direct as existing_rev,
attributed_gms_direct as gms_direct,
attributed_attr_rev_direct as rev_direct,
attributed_receipts_direct as conversions_direct,
attributed_lapsed_receipts_direct as lapsed_conversions_direct,
attributed_new_receipts_direct as new_conversions_direct,
attributed_existing_receipts_direct as existing_conversions_direct,
attributed_new_gms_direct as new_gms_direct,
attributed_lapsed_gms_direct as lapsed_gms_direct,
attributed_existing_gms_direct as existing_gms_direct,
attributed_new_attr_rev_direct as new_rev_direct,
attributed_lapsed_attr_rev_direct as lapsed_rev_direct,
attributed_existing_attr_rev_direct as existing_rev_direct,
program
from `etsy-data-warehouse-dev.rollups.influencer_favorites_overview` a
join get_program b on a.ep_page_title = b.ep_page_title and b.rank = 1
where type in ('influencer marketing', 'influencer affiliate marketing')
union all
select a.ep_page_title,
ep_publish_date,
slug,
a.ep_page_title as slug_adjusted,
type,
run_date,
platform_device,
third_channel,
second_channel,
top_channel,
channel_group,
utm_campaign,
utm_custom2,
utm_medium,
utm_source,
utm_content,
marketing_region,
key_market,
visit_market,
buyer_type_detail,
target_gender,
visits,
insession_gms,
null,
insession_conversions,
insession_lapsed_conversions,
insession_new_conversions,
insession_existing_conversions,
insession_new_gms,
insession_lapsed_gms,
insession_existing_gms,
null,
null,
null,
insession_gms_direct,
null,
insession_conversions_direct,
insession_lapsed_conversions_direct,
insession_new_conversions_direct,
insession_existing_conversions_direct,
insession_new_gms_direct,
insession_lapsed_gms_direct,
insession_existing_gms_direct,
null,
null,
null,
program
from `etsy-data-warehouse-dev.rollups.influencer_favorites_overview` a
join get_program b on a.ep_page_title = b.ep_page_title and b.rank = 1
where type = 'listing engagement'
union all
select a.ep_page_title,
ep_publish_date,
slug,
a.ep_page_title as slug_adjusted,
type,
run_date,
platform_device,
third_channel,
second_channel,
top_channel,
channel_group,
utm_campaign,
utm_custom2,
utm_medium,
utm_source,
utm_content,
marketing_region,
key_market,
visit_market,
buyer_type_detail,
target_gender,
visits,
insession_gms_direct,
null,
insession_conversions_direct,
insession_lapsed_conversions_direct,
insession_new_conversions_direct,
insession_existing_conversions_direct,
insession_new_gms_direct,
insession_lapsed_gms_direct,
insession_existing_gms_direct,
null,
null,
null,
insession_gms_direct,
null,
insession_conversions_direct,
insession_lapsed_conversions_direct,
insession_new_conversions_direct,
insession_existing_conversions_direct,
insession_new_gms_direct,
insession_lapsed_gms_direct,
insession_existing_gms_direct,
null,
null,
null,
program
from `etsy-data-warehouse-dev.rollups.influencer_favorites_overview` a
join get_program b on a.ep_page_title = b.ep_page_title and b.rank = 1
where type = 'page views');

END

SELECT date(timestamp_seconds(run_date)) as date,buyer_type_detail, target_gender,sum(gms_direct) as gms_direct
FROM `etsy-data-warehouse-dev.rollups.influencer_favorites_overview_toplevel` 
group by 1,2,3
