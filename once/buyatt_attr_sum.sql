-- owner: vbhuta@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: this script aggregates buyer attribution data to either the visit date or the purchase date level

begin

declare start_date, clv_date int64;

declare loop_process_month timestamp;

set clv_date = 0;


-- declare clv_date_d `date`;
-- --  set start_date = unix_date(date_trunc(date_sub(current_date(), interval 27 month), month)) * 86400;
-- set start_date = unix_date(date_trunc(date_sub(current_date, interval 27 month), month)) * 86400;

-- /*for full loads- or run drop_incremental_buyatt_tables adhoc script which does same 
-- drop table if exists `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup`;
-- drop table if exists `etsy-data-warehouse-prod.buyatt_mart.attr_sum_visit_date`;
-- drop table if exists `etsy-data-warehouse-prod.buyatt_mart.attr_sum_purchase_date`;
-- */



-- -- create tables in case they dont exist
-- create table if not exists `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup`
-- (
--     buy_date int64,
--     o_visit_run_date int64 ,
--     o_visit_id string,
--     buyer_type string,
--     decay_all float64,
--     decay_all_gms numeric,
--     decay_all_gms_gross numeric,
--     decay_all_attr_rev float64,
--     paid_decay_all float64,
--     paid_decay_all_gms numeric,
--     paid_decay_all_gms_gross numeric,
--     paid_decay_all_attr_rev float64,
--     external_source_decay_all float64,
--     external_source_decay_all_gms numeric,
--     external_source_decay_all_gms_gross numeric,
--     external_source_decay_all_attr_rev float64,
--     external_source_decay_all_etsy_ads_revenue numeric,
--     external_source_decay_all_etsy_ads_revenue_not_charged numeric,
--     external_source_decay_all_ltv_rev numeric
-- )
-- partition by range_bucket(o_visit_run_date, GENERATE_ARRAY(1356998400, 1893456000, 86400))
-- cluster by o_visit_run_date, o_visit_id;

-- create table if not exists `etsy-data-warehouse-prod.buyatt_mart.attr_sum_visit_date`
-- (run_date int64 not null ,
--     device string,
--     third_channel string,
--     second_channel string,
--     top_channel string,
--     channel_group string,
--     utm_campaign string,
--     utm_custom2 string,
--     utm_medium string,
--     utm_source string,
--     utm_content string,
--     marketing_region string,
--     key_market string ,
--     landing_event string,
--     visit_market string,
--     buyer_type string,
--     decay_all float64,
--     decay_all_gms numeric,
--     decay_all_gms_gross numeric,
--     decay_all_attr_rev float64,
--     paid_decay_all float64,
--     paid_decay_all_gms numeric,
--     paid_decay_all_gms_gross numeric,
--     paid_decay_all_attr_rev float64,
--     external_source_decay_all float64,
--     external_source_decay_all_gms numeric,
--     external_source_decay_all_gms_gross numeric,
--     external_source_decay_all_attr_rev float64,
--     external_source_decay_all_etsy_ads_revenue numeric,
--     external_source_decay_all_etsy_ads_revenue_not_charged numeric,
--     external_source_decay_all_ltv_rev numeric
-- )
-- partition by range_bucket(run_date, GENERATE_ARRAY(1356998400, 1893456000, 86400))
-- cluster by run_date, device;

-- create table if not exists `etsy-data-warehouse-prod.buyatt_mart.attr_sum_purchase_date`
-- (buy_date int64 not null ,
--     device string,
--     third_channel string,
--     second_channel string,
--     top_channel string,
--     channel_group string,
--     utm_campaign string,
--     utm_custom2 string,
--     utm_medium string,
--     utm_source string,
--     utm_content string,
--     marketing_region string,
--     key_market string ,
--     landing_event string,
--     visit_market string,
--     buyer_type string,
--     decay_all float64,
--     decay_all_gms numeric,
--     decay_all_gms_gross numeric,
--     decay_all_attr_rev float64,
--     paid_decay_all float64,
--     paid_decay_all_gms numeric,
--     paid_decay_all_gms_gross numeric,
--     paid_decay_all_attr_rev float64,
--     external_source_decay_all float64,
--     external_source_decay_all_gms numeric,
--     external_source_decay_all_gms_gross numeric,
--     external_source_decay_all_attr_rev float64,
--     external_source_decay_all_etsy_ads_revenue numeric,
--     external_source_decay_all_etsy_ads_revenue_not_charged numeric,
--     external_source_decay_all_ltv_rev numeric
-- )
-- partition by range_bucket(buy_date, GENERATE_ARRAY(1356998400, 1893456000, 86400))
-- cluster by buy_date, device;

-- -- delete 27 months for daily processing
-- delete from `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup`
-- where o_visit_run_date >= start_date;

-- -- set clv_date in case there was an earlier delete
-- set clv_date = (select coalesce(max(o_visit_run_date),0) from `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup`);
-- set clv_date_d = date(timestamp_seconds(clv_date)); 

-- delete from `etsy-data-warehouse-prod.buyatt_mart.attr_sum_visit_date`
-- where run_date > clv_date;

-- delete from `etsy-data-warehouse-prod.buyatt_mart.attr_sum_purchase_date`
-- where buy_date > clv_date;

-- -- in case of changes to buyatt_mart.visits view, or visits backfill, data wil be deleted 
-- -- from 3 tables and it will be reloaded here. set dates to check for deleted data. for full
-- -- load, just delete all data from tables, or drop the tables

-- -- this table is temporary, to simulate vertica 
-- /*
-- create table estein_temp.visits_clv as (
-- select o_visit_run_date,
-- o_visit_id,
-- receipt_id,
-- attr_rev,
-- etsy_ads_revenue,
-- etsy_ads_revenue_not_charged
-- from `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` n 
-- join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` 
-- using (receipt_id)
-- where date(timestamp_seconds(n.buy_date)) < date_sub(date_trunc(current_date, month), interval 27 month) );*/


-- -- just to simulate vertica inconsistency, use straight join for prior to 2018
-- insert into `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup`
-- select n.buy_date,
--     n.o_visit_run_date,
--     n.o_visit_id,
--     n.buyer_type,
--     sum(decay_all) as decay_all,
--     cast(sum(decay_all*gms) as numeric) as decay_all_gms,
--     cast(sum(decay_all*gms_gross) as numeric) as decay_all_gms_gross,
--     sum(decay_all*attr_rev) as decay_all_attr_rev,
--     sum(paid_decay_all) as paid_decay_all,
--     cast(sum(paid_decay_all*gms) as numeric) as paid_decay_all_gms,
--     cast(sum(paid_decay_all*gms_gross) as numeric) as paid_decay_all_gms_gross,
--     sum(paid_decay_all*attr_rev) as paid_decay_all_attr_rev,
--     sum(external_source_decay_all) as external_source_decay_all,
--     cast(sum(external_source_decay_all*gms) as numeric) as external_source_decay_all_gms,
--     cast(sum(external_source_decay_all*gms_gross) as numeric) as external_source_decay_all_gms_gross,
--     sum(external_source_decay_all*attr_rev) as external_source_decay_all_attr_rev,
--     cast(sum(external_source_decay_all*etsy_ads_revenue) as numeric) as external_source_decay_all_etsy_ads_revenue,
--     cast(sum(external_source_decay_all*etsy_ads_revenue_not_charged) as numeric) as external_source_decay_all_etsy_ads_revenue_not_charged,
--     cast(sum(external_source_decay_all*ltv_rev) as numeric) as external_source_decay_all_ltv_rev
-- from `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` n
-- join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c
-- --join estein_temp.visits_clv c  -- to simulate vertica 
-- on n.receipt_id = c.receipt_id
-- --and n.o_visit_id = c.o_visit_id    -- remove
-- --and n.o_visit_run_date = c.o_visit_run_date   -- remove
-- and n.o_visit_run_date > clv_date
-- and c.purchase_date > clv_date_d
-- where date(timestamp_seconds(n.buy_date)) < date_sub(date_trunc(current_date, month), interval 27 month) 
-- group by 
--     n.buy_date,
--     n.o_visit_run_date,
--     n.o_visit_id,
--     buyer_type;


-- insert into `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup`
-- select n.buy_date,
--     n.o_visit_run_date,
--     n.o_visit_id,
--     n.buyer_type,
--     sum(decay_all) as decay_all,
--     cast(sum(decay_all*gms) as numeric) as decay_all_gms,
--     cast(sum(decay_all*gms_gross) as numeric) as decay_all_gms_gross,
--     sum(decay_all*attr_rev) as decay_all_attr_rev,
--     sum(paid_decay_all) as paid_decay_all,
--     cast(sum(paid_decay_all*gms) as numeric) as paid_decay_all_gms,
--     cast(sum(paid_decay_all*gms_gross) as numeric) as paid_decay_all_gms_gross,
--     sum(paid_decay_all*attr_rev) as paid_decay_all_attr_rev,
--     sum(external_source_decay_all) as external_source_decay_all,
--     cast(sum(external_source_decay_all*gms) as numeric) as external_source_decay_all_gms,
--     cast(sum(external_source_decay_all*gms_gross) as numeric) as external_source_decay_all_gms_gross,
--     sum(external_source_decay_all*attr_rev) as external_source_decay_all_attr_rev,
--     cast(sum(external_source_decay_all*etsy_ads_revenue) as numeric) as external_source_decay_all_etsy_ads_revenue,
--     cast(sum(external_source_decay_all*etsy_ads_revenue_not_charged) as numeric) as external_source_decay_all_etsy_ads_revenue_not_charged,
--     cast(sum(external_source_decay_all*ltv_rev) as numeric) as external_source_decay_all_ltv_rev
-- from `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` n
-- left join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` c
-- on n.receipt_id = c.receipt_id
-- and n.o_visit_run_date > clv_date
-- and c.purchase_date > clv_date_d
-- where date(timestamp_seconds(n.buy_date)) >= date_sub(date_trunc(current_date, month), interval 27 month) 
-- group by 
--     n.buy_date,
--     n.o_visit_run_date,
--     n.o_visit_id,
--     buyer_type;


--start with clv date, then keep adding

--process data one month at a time

SET loop_process_month = timestamp_trunc(timestamp_seconds(clv_date), month);

WHILE loop_process_month <= timestamp_trunc(current_timestamp, month) DO

-- now join visits summary with visits to get visit attributes
create or replace temp table attr_sum_all
(run_date int64 not null ,
    buy_date int64 not null,
    device string,
    third_channel string,
    second_channel string,
    top_channel string,
    channel_group string,
    utm_campaign string,
    utm_custom2 string,
    utm_medium string,
    utm_source string,
    utm_content string,
    marketing_region string,
    key_market string ,
    landing_event string,
    visit_market string,
    buyer_type string,
    decay_all float64,
    decay_all_gms numeric,
    decay_all_gms_gross numeric,
    decay_all_attr_rev float64,
    paid_decay_all float64,
    paid_decay_all_gms numeric,
    paid_decay_all_gms_gross numeric,
    paid_decay_all_attr_rev float64,
    external_source_decay_all float64,
    external_source_decay_all_gms numeric,
    external_source_decay_all_gms_gross numeric,
    external_source_decay_all_attr_rev float64,
    external_source_decay_all_etsy_ads_revenue numeric,
    external_source_decay_all_etsy_ads_revenue_not_charged numeric,
    external_source_decay_all_ltv_rev numeric
)
partition by range_bucket(run_date, GENERATE_ARRAY(1356998400, 1893456000, 86400))
cluster by run_date, buy_date, device, third_channel as (
select v.run_date,
    a.buy_date,
    mapped_platform_type as device,
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
    landing_event,
    visit_market,
    buyer_type,
    sum(decay_all) as decay_all,
    cast(sum(decay_all_gms) as numeric) as decay_all_gms,
    cast(sum(decay_all_gms_gross) as numeric) as decay_all_gms_gross,
    sum(decay_all_attr_rev) as decay_all_attr_rev,
    sum(paid_decay_all) as paid_decay_all,
    cast(sum(paid_decay_all_gms) as numeric) as paid_decay_all_gms,
    cast(sum(paid_decay_all_gms_gross) as numeric) as paid_decay_all_gms_gross,
    sum(paid_decay_all_attr_rev) as paid_decay_all_attr_rev,
    sum(external_source_decay_all) as external_source_decay_all,
    cast(sum(external_source_decay_all_gms) as numeric) as external_source_decay_all_gms,
    cast(sum(external_source_decay_all_gms_gross) as numeric) as external_source_decay_all_gms_gross,
    sum(external_source_decay_all_attr_rev) as external_source_decay_all_attr_rev,
    cast(sum(external_source_decay_all_etsy_ads_revenue) as numeric) as external_source_decay_all_etsy_ads_revenue,
    cast(sum(external_source_decay_all_etsy_ads_revenue_not_charged) as numeric) as external_source_decay_all_etsy_ads_revenue_not_charged,
    cast(sum(external_source_decay_all_ltv_rev) as numeric) as external_source_decay_all_ltv_rev
from
    `etsy-data-warehouse-prod.buyatt_mart.visit_clv_rollup` a
join
    `etsy-data-warehouse-prod.buyatt_mart.visits` v
on v.run_date = a.o_visit_run_date
and v.visit_id = a.o_visit_id
where a.o_visit_run_date > clv_date 
and v.run_date > clv_date and
timestamp_trunc(timestamp_seconds(a.o_visit_run_date), month) = loop_process_month
and unix_seconds(timestamp_trunc(timestamp_seconds(v.run_date), month)) = loop_process_month
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17)
;

-- now join visits summary with visits to get visit attributes
insert into `etsy-data-warehouse-prod.buyatt_mart.attr_sum_visit_date`
select run_date,
    device,
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
    landing_event,
    visit_market,
    buyer_type,
    sum(decay_all) as decay_all,
    sum(decay_all_gms) as decay_all_gms,
    sum(decay_all_gms_gross) as decay_all_gms_gross,
    sum(decay_all_attr_rev) as decay_all_attr_rev,
    sum(paid_decay_all) as paid_decay_all,
    sum(paid_decay_all_gms) as paid_decay_all_gms,
    sum(paid_decay_all_gms_gross) as paid_decay_all_gms_gross,
    sum(paid_decay_all_attr_rev) as paid_decay_all_attr_rev,
    sum(external_source_decay_all) as external_source_decay_all,
    sum(external_source_decay_all_gms) as external_source_decay_all_gms,
    sum(external_source_decay_all_gms_gross) as external_source_decay_all_gms_gross,
    sum(external_source_decay_all_attr_rev) as external_source_decay_all_attr_rev,
    sum(external_source_decay_all_etsy_ads_revenue) as external_source_decay_all_etsy_ads_revenue,
    sum(external_source_decay_all_etsy_ads_revenue_not_charged) as external_source_decay_all_etsy_ads_revenue_not_charged,
    sum(external_source_decay_all_ltv_rev) as external_source_decay_all_ltv_rev
from
    attr_sum_all a
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;

-- now join visits summary with visits to get visit attributes
insert into `etsy-data-warehouse-prod.buyatt_mart.attr_sum_purchase_date`
select buy_date,
    device,
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
    landing_event,
    visit_market,
    buyer_type,
    sum(decay_all) as decay_all,
    sum(decay_all_gms) as decay_all_gms,
    sum(decay_all_gms_gross) as decay_all_gms_gross,
    sum(decay_all_attr_rev) as decay_all_attr_rev,
    sum(paid_decay_all) as paid_decay_all,
    sum(paid_decay_all_gms) as paid_decay_all_gms,
    sum(paid_decay_all_gms_gross) as paid_decay_all_gms_gross,
    sum(paid_decay_all_attr_rev) as paid_decay_all_attr_rev,
    sum(external_source_decay_all) as external_source_decay_all,
    sum(external_source_decay_all_gms) as external_source_decay_all_gms,
    sum(external_source_decay_all_gms_gross) as external_source_decay_all_gms_gross,
    sum(external_source_decay_all_attr_rev) as external_source_decay_all_attr_rev,
    sum(external_source_decay_all_etsy_ads_revenue) as external_source_decay_all_etsy_ads_revenue,
    sum(external_source_decay_all_etsy_ads_revenue_not_charged) as external_source_decay_all_etsy_ads_revenue_not_charged,
    sum(external_source_decay_all_ltv_rev) as external_source_decay_all_ltv_rev
from
    attr_sum_all a
where buy_date > loop_start_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;

SET loop_process_month = timestamp(date_add(date(loop_process_month), interval 1 month));

END WHILE;

end; 
