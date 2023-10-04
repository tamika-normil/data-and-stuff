SELECT date_trunc(creation_tsz, quarter) as date, sum(gms_net) as gms
FROM ( select distinct listing_id, case when quarter = 'Q1' then date('2023-03-27')
when quarter = 'Q2' then date('2023-04-25') 
when quarter = 'Q3' then date('2023-08-21') 
end as start_date
from `etsy-data-warehouse-dev.tnormil.martha_listings`)
left join etsy-data-warehouse-prod.transaction_mart.all_transactions using (listing_id)
left join etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans using (transaction_id)
where date(creation_tsz) >= start_date
group by 1
order by 1;

begin

# this replicates code we use to calculate visits for the core influencer looker dashboard
# https://github.com/etsy/Rollups/blob/6bca299b65d1ca83ba2aacea48ace34e9b32c709/auto/p3/daily/influencer_favorites_overview.sql

DECLARE program_start_date date;

set program_start_date = DATE('2023-03-27') ;

create temp table collection_ep as
(with ep as (select REGEXP_REPLACE(lower(slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$', "")  as ep_page_title, min(cast(timestamp_seconds(publish_date) as date)) as ep_publish_date
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
join `etsy-data-warehouse-prod.etsy_index.finds_page`fp on ep.ep_page_title  = REGEXP_REPLACE(lower(fp.slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$', "") 
where publish_date <> 0
and slug like '%marthastewart%');

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
join (select distinct ep_page_title, ep_publish_date, slug from collection_ep) cl on lower(cl.slug ) like '%' || lower(c.page_title) || '%' and REGEXP_CONTAINS(lower(cl.slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$') is false
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
join (select distinct ep_page_title, ep_publish_date, slug from collection_ep) cl on lower(cl.slug ) like '%' || lower(c.page_title) || '%' and REGEXP_CONTAINS(lower(cl.slug), r'-uk$|-ca$|-de$|-fr$|-au$|-us$') is false
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

SELECT date(date_trunc(creation_tsz, quarter)) as date, min(date(creation_tsz)) as start_date, max(date(creation_tsz)) as end_date, sum(gms_net) as gms,
sum(case when cv.visit_id is not null then gms_net else 0 end) as gms_attr_martha
FROM ( select distinct listing_id, case when quarter = 'Q1' then date('2023-03-27')
when quarter = 'Q2' then date('2023-04-25') 
when quarter = 'Q3' then date('2023-08-21') 
end as start_date
from `etsy-data-warehouse-dev.tnormil.martha_listings`)
left join etsy-data-warehouse-prod.transaction_mart.all_transactions att using (listing_id)
left join etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans using (transaction_id)
left join etsy-data-warehouse-prod.transaction_mart.transactions_visits tv using (transaction_id)
left join collection_visits cv on tv.visit_id = cv.visit_id
and creation_tsz > cv.start_datetime
where date(att.creation_tsz) >= start_date
group by 1
order by 1;

end;
