begin

declare date1, date2 date;
set date1 = date_sub(current_date, interval 3 year);  -- change to current_date
set date2 = date_sub(current_date, interval 1 day);   -- change to current_date

create or replace TABLE `etsy-data-warehouse-dev.tnormil.order_attribution_slice_new` 
partition by _date as
WITH eligible_orders AS
(
       SELECT _date,
       run_date, 
       day_format,
       visit_time,
       browser_id, 
       visit_id,
       order_id,
       row_number() OVER (PARTITION BY order_id ORDER BY visit_time) AS rn
FROM `etsy-data-warehouse-prod.weblog.order_attribution`
WHERE _date between date1 and date2 
)
SELECT _date,
       run_date, 
       day_format,
       visit_time,
       browser_id, 
       visit_id,
       order_id 
FROM eligible_orders
WHERE rn = 1;

end;

-- owner: marketinganalytics@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: attributes revenue from the receipts in order_attribution_slice to the channels of the buyers' visits 

------------------------------------------------------
-- BUY VISITS + PRIOR VISITS + RECEIPT INFO
------------------------------------------------------

-- this will process the date range that is extracted in order_attribution_slice
-- creates 3 temp tables and joins them in receipts_visits_slice, which contains same date
-- range as order_attribution_slice. this is later loaded into receipts_visits.
-- uses buyatt_mart.visits view on weblog.visits for mapped visit columns


begin

declare prior_date1, prior_date2, curr_date1, curr_date2, min_date int64;

create temporary table buyatt_receipt_slice as (
select
    o.run_date,
    o.day_format as day_key,
    o.browser_id,
    o.visit_id as buy_visit_id,
    timestamp_seconds(o.visit_time) as buy_visit_timestamp,
    o.order_id,
    r.buyer_user_id,
    r.receipt_id,
    r.creation_tsz as receipt_timestamp,
    r.receipt_market,
    r.gms,
    r.gms_gross,
    case
        when (prior_receipt_tsz is null) then 'new'
        when (prior_receipt_tsz < timestamp_sub(creation_tsz, interval 365 day)) then 'lapsed'
        else 'existing'
        end as buyer_type
from
   `etsy-data-warehouse-dev.tnormil.order_attribution_slice_new` o
join
    `etsy-data-warehouse-prod.buyatt_mart.receipts_w_cum_gms` r
using (order_id));

set min_date = (select min(run_date) from `etsy-data-warehouse-prod.buyatt_mart.order_attribution_slice_new`);

set prior_date1 = (select (unix_date(min(day_key))-90) * 86400 from buyatt_receipt_slice); 
set prior_date2 = (select (unix_date(min(day_key))-30) * 86400 from buyatt_receipt_slice);

set curr_date1 = prior_date2;
set curr_date2 = (select (unix_date(max(day_key))) * 86400 from buyatt_receipt_slice);

create temporary table buy_visit_mapping as (
select buy_visit_id,
    buy_visit_timestamp,
    day_key,
    receipt_id,
    b.browser_id,
    coalesce(b.maps_to_browser, a.browser_id) as maps_to_browser
from buyatt_receipt_slice a 
left join `etsy-data-warehouse-prod.buyatt_mart.unioned_browsers` b
using(browser_id));

-- get max date of visit before window

create temporary table buyatt_priorvisits
    as (
with prior_visits as (
select distinct visit_id,
    v.run_date,
    start_datetime,
    v.browser_id
from
    `etsy-data-warehouse-prod.visit_mart.visits` v
where
    v.browser_id in (select maps_to_browser from buy_visit_mapping)
    and v.run_date between prior_date1 and prior_date2 
    and v.platform_app != 'soe'    -- REMOVING SOE VISITS
) 
select b.buy_visit_id,
    max(p.start_datetime) as max_visit_before_window
from
    buy_visit_mapping b
join
    prior_visits p
on b.maps_to_browser = p.browser_id
where
    p.start_datetime < timestamp_sub(b.buy_visit_timestamp, interval 30 day)
    and p.start_datetime > timestamp_sub(b.buy_visit_timestamp, interval 90 day)
group by b.browser_id, b.buy_visit_id);

-- get visit data for all visits within 30 days of purchase visit

create temporary table buyatt_currvisits
    as (
with window_visits as (
select
    v.browser_id,
    v.run_date,
    start_datetime,
    visit_id,
    case when (second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'intl_css_plas', 'affiliates'
                   ) or (second_channel in ('native_display','intl_native_display') and third_channel not like '%discovery%'))
          AND upper(utm_campaign) NOT LIKE '%_CUR_%' then 1 else 0 end as paid,
          #(second_channel in ('affiliates') and third_channel in ('affiliates_widget','affiliates_feed'))
    has_referral,
    external_source
from
    `etsy-data-warehouse-prod.buyatt_mart.visits` v
where
    v.browser_id in (select maps_to_browser from buy_visit_mapping)
    and v.run_date between curr_date1 and curr_date2
    and v.platform_app != 'soe'    -- REMOVING SOE VISITS
)
select
    b.browser_id,
    b.buy_visit_id,
    b.receipt_id,
    w.visit_id,
    w.start_datetime,
    w.run_date,
    w.paid,
    w.has_referral,
    w.external_source
from
    buy_visit_mapping b
join
    window_visits w
on b.maps_to_browser = w.browser_id
where w.start_datetime <= b.buy_visit_timestamp
and w.start_datetime >= timestamp_sub(b.buy_visit_timestamp , interval 30 day)
and w.run_date <= unix_date(day_key) * 86400
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.attr_by_browser`
(
    buy_date INT64,
    receipt_id INT64,
    receipt_timestamp TIMESTAMP,
    receipt_market STRING,
    buy_visit_id STRING,
    buyer_type STRING,
    gms NUMERIC,
    gms_gross NUMERIC,
    o_visit_id STRING,
    o_visit_run_date INT64,
    decay_factor FLOAT64,
    paid INT64,
    has_referral INT64,
    external_source INT64,
    last_click_all INT64,
    decay_all FLOAT64,
    paid_last_click_all INT64,
    paid_decay_all FLOAT64,
    has_referral_decay_all FLOAT64,
    external_source_decay_all FLOAT64
)
PARTITION BY RANGE_BUCKET(o_visit_run_date, GENERATE_ARRAY(1385942400, 2077056000, 86400))
CLUSTER BY o_visit_id;

#delete from `etsy-data-warehouse-.buyatt_mart.attr_by_browser` where buy_date >= min_date;

insert into `etsy-data-warehouse-dev.tnormil.attr_by_browser`
(buy_date,
 receipt_id,
 receipt_timestamp,
 receipt_market,
 buy_visit_id,
 buyer_type,
 gms,
 gms_gross,
 o_visit_id,
 o_visit_run_date,
 decay_factor,
 paid,
 has_referral,
 external_source,
 last_click_all,
 decay_all,
 paid_last_click_all,
 paid_decay_all,
 has_referral_decay_all,
 external_source_decay_all)
with receipts as (
select 
       distinct receipt_id,
       unix_date(day_key) * 86400 as buy_date,
       buyer_user_id,
       buyer_type,
       ri.buy_visit_id,
       buy_visit_timestamp,
       receipt_timestamp,
       receipt_market,
       browser_id,
       gms,
       gms_gross,
       p.max_visit_before_window
from buyatt_receipt_slice ri
left join buyatt_priorvisits p
on ri.buy_visit_id = p.buy_visit_id),
visits as (
select c.buy_visit_id,
    c.receipt_id,
    c.visit_id,
    c.start_datetime,
    c.run_date,
    c.paid,
    c.has_referral,
    c.external_source
from
    buyatt_currvisits c
),
factors as (
select r.buy_date,
    r.buyer_user_id,
    r.buyer_type,
    r.receipt_id,
    r.buy_visit_id,
--    r.buy_visit_timestamp,
    r.receipt_timestamp,
    r.gms,
    r.gms_gross,
    r.receipt_market,
    r.max_visit_before_window,
    v.visit_id as o_visit_id,
    v.start_datetime as o_visit_timestamp,
    v.run_date as o_visit_run_date,
    case
      when (max_visit_before_window is null 
        or timestamp_diff(buy_visit_timestamp, max_visit_before_window, second) > 5184000)
        and (row_number() over 
            (partition by r.receipt_id order by v.start_datetime, v.visit_id) = 1)
      then
          exp(-0.099 * ((UNIX_SECONDS(r.receipt_timestamp) - UNIX_SECONDS(buy_visit_timestamp))/86400.0))
          -- exp(-0.099*buy_visit_timestamp)
      else
        exp(-0.099 * ((UNIX_SECONDS(r.receipt_timestamp) - UNIX_SECONDS(v.start_datetime))/86400.0))
        -- exp(-0.099*o_visit_timestamp)
    end as decay_factor,
    v.paid,
    v.has_referral,
    v.external_source
from
    receipts r
inner join
    visits v
on r.receipt_id = v.receipt_id
and r.buy_visit_id = v.buy_visit_id)
,factors_with_logic as (
select buy_date,
    receipt_id,
    receipt_timestamp,
    receipt_market,
    buy_visit_id,
    buyer_type,
    gms,
    gms_gross,
    o_visit_id,
    o_visit_run_date,
    decay_factor,
    paid,
    has_referral,
    external_source,
    -- all channel logic
    case when (row_number() over (partition by receipt_id order by o_visit_timestamp desc) = 1) then 1 else 0 end as last_click_all,
    decay_factor / sum(decay_factor) over (partition by receipt_id) as decay_all,
    -- paid channel logic
    case when (row_number() over (partition by receipt_id, paid order by o_visit_timestamp desc) = 1 
      and paid = 1) then 1 else 0 end as paid_last_click_all,
    case when paid = 1 then decay_factor else 0 end as paid_decay_factor,
    sum(case when paid = 1 then decay_factor else 0 end) over (partition by receipt_id) as paid_decay_factor_sum, 
    -- has_referral channel logic
     case when (row_number() over (partition by receipt_id, has_referral order by o_visit_timestamp desc) = 1
      and has_referral = 1) then 1 else 0 end as has_referral_last_click_all,
    case when has_referral = 1 then decay_factor else 0 end as has_referral_decay_factor,
    sum(case when has_referral = 1 then decay_factor else 0 end) over (partition by receipt_id) as has_referral_decay_factor_sum,
    -- external_source channel logic
    case when (row_number() over (partition by receipt_id, external_source order by o_visit_timestamp desc) = 1
      and external_source = 1) then 1 else 0 end as external_source_last_click_all,
    case when external_source = 1 then decay_factor else 0 end as external_source_decay_factor,
    sum(case when external_source = 1 then decay_factor else 0 end) over (partition by receipt_id) as external_source_decay_factor_sum
from factors)
select buy_date,
    receipt_id,
    receipt_timestamp,
    receipt_market,
    buy_visit_id,
    buyer_type,
    ROUND(gms, 2) AS gms,
    ROUND(gms_gross, 2) AS gms_gross,
    o_visit_id,
    o_visit_run_date,
    decay_factor,
    paid,
    has_referral,
    external_source,
    -- all channels
    last_click_all,
    decay_all,
    -- paid
    case
      when paid_decay_factor_sum > 0 then paid_last_click_all
      when has_referral_decay_factor_sum > 0 then has_referral_last_click_all
      when external_source_decay_factor_sum > 0 then external_source_last_click_all
      else last_click_all
    end as paid_last_click_all,
    case
      when paid_decay_factor_sum > 0 then paid_decay_factor / paid_decay_factor_sum
      when has_referral_decay_factor_sum > 0 then has_referral_decay_factor / has_referral_decay_factor_sum
      when external_source_decay_factor_sum > 0 then external_source_decay_factor / external_source_decay_factor_sum
      else decay_all
    end as paid_decay_all,
    -- has_referral
    case
      when has_referral_decay_factor_sum > 0 then has_referral_decay_factor / has_referral_decay_factor_sum
      when external_source_decay_factor_sum > 0 then external_source_decay_factor / external_source_decay_factor_sum
      else decay_all
    end as has_referral_decay_all,
    -- external_source
    case
      when external_source_decay_factor_sum > 0 then external_source_decay_factor / external_source_decay_factor_sum
      else decay_all
    end as external_source_decay_all
from factors_with_logic
;

-- now create table for category attributions
-- create temp table cat_by_channel as (
--     with rcpts as (
--         select distinct receipt_id
--         from buyatt_receipt_slice),
--     trans as (
--         select
--             g.receipt_id
--             ,taxonomy_id
--             ,cast(is_vintage as boolean) as is_vintage
--             ,c.new_category as category
--             ,second_level_cat_new as subcategory
--             ,third_level_cat_new as subsubcategory
--             ,sum(gms_gross) as gms
--             ,count(distinct listing_id) as listings
--         from  
--             rcpts r
--         join  
--             `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` g
--             using (receipt_id)
--         join
--             `etsy-data-warehouse-prod.transaction_mart.all_transactions_categories` c
--             using(transaction_id)
--         where
--             gms_gross > 0
--         group by 1,2,3,4,5,6
--     )
--     select receipt_id
--     ,taxonomy_id
--     ,is_vintage
--     ,listings
--     ,cast (gms/(sum(gms) over (partition by receipt_id)) as numeric) as perc_of_receipt
--     ,coalesce(category, '') as category
--     ,coalesce(subcategory, '') as subcategory
--     ,coalesce(subsubcategory, '') as subsubcategory
-- from trans);


-- delete from `etsy-data-warehouse-prod.buyatt_mart.attr_receipt` where buy_date > min_date;
--
-- insert into `etsy-data-warehouse-prod.buyatt_mart.attr_receipt`
-- (   receipt_id,
--     buy_date,
--     taxonomy_id,
--     is_vintage,
--     listings,
--     perc_of_receipt,
--     category,
--     subcategory,
--     subsubcategory,
--     gms,
--     gms_gross,
--     o_visit_id,
--     o_visit_run_date,
--     decay_all,
--     has_referral_decay_all
-- )
--     select c.receipt_id,
--     a.buy_date,
--     c.taxonomy_id,
--     c.is_vintage,
--     c.listings,
--     c.perc_of_receipt,
--     c.category,
--     c.subcategory,
--     c.subsubcategory,
--     a.gms,
--     a.gms_gross,
--     a.o_visit_id,
--     a.o_visit_run_date,
--     a.decay_all,
--     a.has_referral_decay_all
-- from cat_by_channel c
-- join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` a
-- using (receipt_id)
-- where a.buy_date > min_date
-- ;

end;
