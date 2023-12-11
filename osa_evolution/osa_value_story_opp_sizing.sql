-- create base data

create or replace temporary table opp_sizing_shops as
    (with valid_shops as 
    (SELECT s.shop_id, 30 as attribution_window, max(distinct case when syndicated = true then 1 else 0 end) as is_syndicated
    FROM `etsy-data-warehouse-prod.rollups.seller_basics` s
    left join etsy-data-warehouse-prod.olf.listing_fact_daily l on s.shop_id = l.shop_id and _PARTITIONDATE >= current_date - 2
    where active_seller_status = 1
    group by 1,2),
    clicks as 
    (select shop_id, count(*) as clicks
    from `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks`
    where date(timestamp_seconds(click_date)) >= current_date - 30
    group by 1),
    sales as
    (select shop_id, count(*) as sales
    from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts
    `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts`
    left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r using (receipt_id)
    where date(timestamp_seconds(purchase_date)) >= current_date - 30 
    and status = 1
    and receipt_live = 1
    group by 1)
    select is_syndicated, attribution_window, shop_id, clicks,
    case when c.shop_id is not null then 1 else 0 end as has_clicks,
    case when s.shop_id is not null then 1 else 0 end as has_sales
    from valid_shops v
    left join clicks c using (shop_id)
    left join sales s using (shop_id));

create or replace table etsy-data-warehouse-dev.tnormil.opp_sizing_stats as 
(with osa as
    (select shop_id, s.user_id as seller_user_id, 
     sum(gms_usd/100) as chargeable_gms,
     sum(acquisition_fee_usd/100) as ad_revenue,
     count(distinct receipt_id) as chargeable_orders
    from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts a
    left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (shop_id)
    left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r using (receipt_id)
    where receipt_live = 1
    and status = 1
    and date(timestamp_seconds(purchase_date)) >= current_date - 30
    group by 1,2),
comm as
    (select shop_id, seller_user_id, sum(gms_net) as gms_net,
    count(distinct receipt_id) as orders,
    sum(case when date(creation_tsz) >= DATE('2022-04-11') then coalesce(gms_net,0)*0.115 else coalesce(gms_net,0)*0.102 end) as comm_rev
    from etsy-data-warehouse-prod.transaction_mart.receipts_gms r
    left join etsy-data-warehouse-prod.rollups.seller_basics_all s on r.seller_user_id = s.user_id 
    where date(creation_tsz) >= current_date - 30
    group by 1,2),
prolist as
    (select shop_id, l.user_id as seller_user_id, sum((p.cost) / 100 ) as prolist_cost,
     from etsy-data-warehouse-prod.rollups.prolist_click_visits p
     left join etsy-data-warehouse-prod.listing_mart.listing_vw l using (listing_id)
     where _date >= current_date - 30
     group by 1,2),
osa_ds_engage as 
    (select shop_id, count(*) as events, count(distinct date(TIMESTAMP_MILLIS(epoch_ms))) as days_visited
    from
    `etsy-data-warehouse-prod.weblog.events` e
    left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (user_id)
    where event_type in ('external_ads_landing')
    and date(TIMESTAMP_MILLIS(epoch_ms)) >= current_date - 30 
    group by 1),
ds_engage as 
    (select shop_id, count(*) as events, count(distinct date(TIMESTAMP_MILLIS(epoch_ms))) as days_visited
    from
    `etsy-data-warehouse-prod.weblog.events` e
    left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (user_id)
    where event_type in ('mc_seller_dashboard_legacy')
    and date(TIMESTAMP_MILLIS(epoch_ms)) >= current_date - 30 
    group by 1),
etsy_ads_engage as 
    (select shop_id, count(*) as events, count(distinct date(TIMESTAMP_MILLIS(epoch_ms))) as days_visited
    from
    `etsy-data-warehouse-prod.weblog.events` e
    left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (user_id)
    where event_type in ('ad_vector_landing')
    and date(TIMESTAMP_MILLIS(epoch_ms)) >= current_date - 30 
    group by 1)
select o.*, c.gms_net, coalesce(c.comm_rev,0) as comms_rev, coalesce(os.chargeable_gms,0) as chargeable_gms, coalesce(os.ad_revenue,0) as ad_revenue, coalesce(p.prolist_cost,0) as prolist_cost
, coalesce(ods.events,0) as osa_events, coalesce(ods.days_visited,0) as osa_days_visited
, coalesce(ds.events,0) as ds_events, coalesce(ds.days_visited,0) as ds_days_visited
, coalesce(ea.events,0) as ea_events, coalesce(ea.days_visited,0) as ea_days_visited
from opp_sizing_shops o
left join osa os using (shop_id)
left join comm c using (shop_id)
left join prolist p using (shop_id)
left join osa_ds_engage ods using (shop_id)
left join ds_engage ds using (shop_id)
left join etsy_ads_engage ea using  (shop_id))


/*

begin

DECLARE attribution_window_number_of_days ARRAY<int64>;
DECLARE attribution_window_number_of_day INT64;
DECLARE i INT64 DEFAULT 0;

SET attribution_window_number_of_days =  [30,60,90,180,365];

LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(attribution_window_number_of_days) THEN 
    LEAVE; 
  END IF;

SET attribution_window_number_of_day = attribution_window_number_of_days[ORDINAL(i)];

if i = 1 then 

create or replace temporary table opp_sizing_shops as
(with valid_shops as 
(SELECT s.shop_id, attribution_window_number_of_day as attribution_window, max(distinct case when syndicated = true then 1 else 0 end) as is_syndicated
FROM `etsy-data-warehouse-prod.rollups.seller_basics` s
left join etsy-data-warehouse-prod.olf.listing_fact_daily l on s.shop_id = l.shop_id and _PARTITIONDATE >= current_date - 2
where active_seller_status = 1
group by 1,2),
clicks as 
(select shop_id, count(*) as clicks
from `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks`
where date(timestamp_seconds(click_date)) >= current_date - attribution_window_number_of_day 
group by 1),
sales as
(select shop_id, count(*) as sales
from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts
`etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts`
where date(timestamp_seconds(purchase_date)) >= current_date - attribution_window_number_of_day 
group by 1)
select is_syndicated, attribution_window, count(distinct shop_id) as shops,
count(distinct case when c.shop_id is not null then shop_id end) as shop_clicks,
count(distinct case when s.shop_id is not null then shop_id end) as shop_sales,
from valid_shops v
left join clicks c using (shop_id)
left join sales s using (shop_id)
group by 1,2);

else 

insert into opp_sizing_shops
(with valid_shops as 
(SELECT s.shop_id, attribution_window_number_of_day as attribution_window, max(distinct case when syndicated = true then 1 else 0 end) as is_syndicated
FROM `etsy-data-warehouse-prod.rollups.seller_basics` s
left join etsy-data-warehouse-prod.olf.listing_fact_daily l on s.shop_id = l.shop_id and _PARTITIONDATE >= current_date - 2
where active_seller_status = 1
group by 1,2),
clicks as 
(select shop_id, count(*) as clicks
from `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks`
where date(timestamp_seconds(click_date)) >= current_date - attribution_window_number_of_day 
group by 1),
sales as
(select shop_id, count(*) as sales
from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts
`etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts`
where date(timestamp_seconds(purchase_date)) >= current_date - attribution_window_number_of_day 
group by 1)
select is_syndicated, attribution_window, count(distinct shop_id) as shops,
count(distinct case when c.shop_id is not null then shop_id end) as shop_clicks,
count(distinct case when s.shop_id is not null then shop_id end) as shop_sales,
from valid_shops v
left join clicks c using (shop_id)
left join sales s using (shop_id)
group by 1,2);

end if;

END LOOP;

end

*/

-- add key fields for reporting

create temp table base as 
(select *, round( safe_divide( cast(gms_net as float64), cast(comms_rev as float64) + cast(ad_revenue as float64) +	
cast(prolist_cost as float64)),1) as roi, 
cast(comms_rev as float64) + cast(ad_revenue as float64) + cast(prolist_cost as float64) as total_costs,
safe_divide( cast(comms_rev as float64) + cast(ad_revenue as float64) +	
cast(prolist_cost as float64), cast(gms_net as float64)) as takerate
from etsy-data-warehouse-dev.tnormil.opp_sizing_stats);

-- shops and ds usage by fee rate

SELECT has_clicks, 
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when total_costs <= 0 or total_costs is null then 0 else 1 end has_cost,
case when cast(ad_revenue as float64) <= 0 or cast(ad_revenue as float64) is null then 0 else 1 end has_ad_revenue,
roi, 
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate < .5 then round(takerate, 1)
else null end as take_rate_bin, 
count(distinct shop_id) as shops,
sum(coalesce(gms_net,0)) as gms_net,
sum( cast(comms_rev as float64) + cast(ad_revenue as float64) +	cast(prolist_cost as float64)) as costs,
count(distinct case when osa_events > 0 then shop_id end) as visited_osa_ds,
count(distinct case when ds_events > 0 then shop_id end) as visited_ds,
count(distinct case when ea_events > 0 then shop_id end) as visited_ea_ds
FROM base
where is_syndicated = 1
group by 1,2,3,4,5,6;

-- costs breakdown by fee rate

SELECT has_clicks, 
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate >= .3 and takerate < .5 then round(takerate, 1)
when takerate < .3 then round(takerate, 2)
else null end as take_rate_bin, 
count(distinct shop_id) as shops,
sum(ad_revenue) as ad_revenue,
sum(prolist_cost) as prolist_cost,
sum(comms_rev) as comms_rev,
sum(total_costs) as total_costs,
sum(gms_net) as gms_net
FROM base
where is_syndicated = 1
group by 1,2,3;

-- avg / stddev clicks + gms

SELECT has_clicks, 
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate >= .3 and takerate < .5 then round(takerate, 1)
when takerate >= .25 and takerate < .3 then .25
when takerate >= .2 and takerate < 25 then .2
when takerate >= .15 and takerate < 2 then .15
when takerate < .15 then round(takerate, 2)
else null end as take_rate_bin, 
count(distinct shop_id) as shops,
avg(clicks) as clicks,
stddev(clicks) as clicks_stddev,
avg(gms_net) as gms_net,
stddev(gms_net) as gms_net_stddev
FROM base
where is_syndicated = 1
and (gms_net > 0 or total_costs > 0)
group by 1,2,3;

--- heuristics 
-- dist of shops by country, days since open, seller tier, top cat + fee rate for sellers with costs

SELECT
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate >= .3 and takerate < .5 then round(takerate, 1)
when takerate >= .25 and takerate < .3 then .25
when takerate >= .2 and takerate < 25 then .2
when takerate >= .15 and takerate < 2 then .15
when takerate < .15 then round(takerate, 2)
else null end as take_rate_bin, 
top_category_new,
seller_tier,
case when date_diff(current_date,open_date, month) < 12 then 0
when date_diff(current_date,open_date, month) >= 48 then 4
else date_diff(current_date,open_date, year) end as days_since_open,
case when country_name in ('United States','United Kingdom','Canada','Germany','France') then country_name else 'RoW' end as country_name, 
count(distinct b.shop_id) as shops,
FROM base b
left join etsy-data-warehouse-prod.rollups.seller_basics s using (shop_id)
where is_syndicated = 1
and (gms_net > 0 or total_costs > 0)
group by 1,2,3,4,5,6;

--- opp sizing 

-- median clicks / prolist cost by fee rate

with agg_base as 
(SELECT has_clicks, 
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when total_costs <= 0 or total_costs is null then 0 else 1 end has_cost,
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate >= .3 and takerate < .5 then round(takerate, 1)
when takerate < .3 then round(takerate, 2)
else null end as take_rate_bin, 
count(distinct shop_id) as shops,
sum(past_year_gms) as past_year_gms,
sum(clicks) as clicks,
FROM base
left join etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where is_syndicated = 1
group by 1,2,3,4),
base as 
(SELECT has_clicks, 
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when total_costs <= 0 or total_costs is null then 0 else 1 end has_cost,
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate >= .3 and takerate < .5 then round(takerate, 1)
when takerate < .3 then round(takerate, 2)
else null end as take_rate_bin, 
clicks, 
prolist_cost,
shop_id
FROM base
left join etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where is_syndicated = 1),
median_base as
(select distinct has_clicks, has_gms, has_cost, take_rate_bin
, PERCENTILE_CONT(clicks, 0.5) over (partition by has_clicks, has_gms, has_cost, cast(take_rate_bin as string)) as median_clicks
, PERCENTILE_CONT(prolist_cost, 0.5) over (partition by has_clicks, has_gms, has_cost, cast(take_rate_bin as string)) as median_prolist_cost
from base)
select a.*, b.median_clicks, b.median_prolist_cost
from agg_base a
left join median_base b using (has_clicks, has_gms, has_cost, take_rate_bin);

-- past year gms by fee rate

SELECT has_clicks, 
case when gms_net <= 0 or gms_net is null then 0 else 1 end has_gms,
case when total_costs <= 0 or total_costs is null then 0 else 1 end has_cost,
case when cast(ad_revenue as float64) <= 0 or cast(ad_revenue as float64) is null then 0 else 1 end has_ad_revenue,
case when takerate >= 1 then 1
when takerate >= .5 and takerate < 1 then .5
when takerate < .5 then round(takerate, 1)
else null end as take_rate_bin, 
count(distinct shop_id) as shops,
sum(past_year_gms) as past_year_gms,
FROM base
left join etsy-data-warehouse-prod.rollups.seller_basics using (shop_id)
where is_syndicated = 1
group by 1,2,3,4,5;
