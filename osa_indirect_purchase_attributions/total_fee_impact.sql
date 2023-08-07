--share of offsite ads visits by seller
--etsy ads and pla fees before and after this update

BEGIN

DECLARE attribution_window_number_of_days ARRAY<int64>;

SET attribution_window_number_of_days =  [1,2,3,7,14,30];

create temp table tier as 
(with seller_tier_daily as 
    (select date_trunc(trans_date, month) as date, seller_user_id, sum(gms_net) as gms_net
    from etsy-data-warehouse-prod.transaction_mart.transactions_gms
    where trans_date >= '2018-01-01'
    group by 1,2),
dates as 
    (SELECT distinct date, seller_user_id
    FROM (select seller_user_id, min(date) as first_sale from seller_tier_daily group by 1)
    left join UNNEST(GENERATE_DATE_ARRAY(first_sale, current_date, INTERVAL 1 month)) AS date)
select distinct d.date, d.seller_user_id, sum(COALESCE(gms_net, 0))
OVER (
    PARTITION BY d.seller_user_id
    ORDER BY d.date
    ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) as past_year_gms  
from dates d
left join seller_tier_daily s using(date, seller_user_id));

create temp table rev_hist as 
(with dates as 
    (SELECT distinct date
    FROM UNNEST(GENERATE_DATE_ARRAY(date_trunc('2022-06-01', month), current_date, INTERVAL 1 month)) AS date),
shop_dates as 
    (SELECT d.date, seller_user_id, max(date_trunc(p.date, month)) as last_purchase_date
    FROM dates d
    join (select date_trunc(date(timestamp_seconds(purchase_date)), month) as date, s.user_id as seller_user_id from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts a
            left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (shop_id)
            union all
         select distinct date_trunc(purchase_date_trunc, month) as date, seller_user_id from etsy-data-warehouse-dev.tnormil.opted_in_incremental_diff_shop_attributions2) p on d.date <= p.date
    group by 1,2
    having last_purchase_date >= date_sub(d.date, interval 12 month)),
shop_dates2 as 
    (SELECT date, seller_user_id, last_purchase_date, attribution_window_number_of_day
    FROM shop_dates
    join unnest(attribution_window_number_of_days) as attribution_window_number_of_day),    
osa as
    (select date_trunc(date(timestamp_seconds(purchase_date)), month) as date, shop_id, s.user_id as seller_user_id, 
     sum(gms_usd/100) as chargeable_gms,
     sum(acquisition_fee_usd/100) as ad_revenue,
     count(distinct receipt_id) as chargeable_orders
    from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts a
    left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (shop_id)
    left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r using (receipt_id)
    where receipt_live = 1
    group by 1,2,3),
comm as
    (select date_trunc(date(creation_tsz), month) as date, seller_user_id, sum(gms_net) as gms_net,
    count(distinct receipt_id) as orders,
    sum(case when date(creation_tsz) >= DATE('2022-04-11') then coalesce(gms_net,0)*0.115 else coalesce(gms_net,0)*0.102 end) as comm_rev
     from etsy-data-warehouse-prod.transaction_mart.receipts_gms
     group by 1,2),
prolist as
    (select date_trunc(_date, month) as date, l.user_id as seller_user_id, sum((p.cost) / 100 ) as prolist_cost,
     from etsy-data-warehouse-prod.rollups.prolist_click_visits p
     left join etsy-data-warehouse-prod.listing_mart.listing_vw l using (listing_id)
     where _date >= '2022-06-01'
     group by 1,2),
inc_osa as
    (select date_trunc(purchase_date_trunc, month) as date, a.seller_user_id, attribution_window as attribution_window_number_of_day,
     sum(gms_net) as inc_gms_net,
     count(distinct receipt_id) as inc_orders
     from etsy-data-warehouse-dev.tnormil.opted_in_incremental_diff_shop_attributions2 a
     where type = 'indirect'
     group by 1,2,3)
select distinct s.date, s.seller_user_id,
s.attribution_window_number_of_day,
chargeable_gms, 
chargeable_orders,
ad_revenue,
comm_rev,
prolist_cost,
gms_net,
orders,
inc_gms_net,
inc_orders
#past_year_gms,
#case when past_year_gms > 0 and past_year_gms < 10 then 10
#when past_year_gms >= 10 then round(past_year_gms, (length(cast(round(past_year_gms) as string))-1)*-1) end as past_year_gms_rd,
from shop_dates2 s
left join osa o using (date, seller_user_id)
left join comm c using (date, seller_user_id)
left join prolist p using (date, seller_user_id)
left join inc_osa io using (date, seller_user_id,attribution_window_number_of_day)
order by seller_user_id, date desc);


create or replace table `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist`  as 
(SELECT r.seller_user_id,
attribution_window_number_of_day,
coalesce(past_year_gms,0) as past_year_gms,
case when past_year_gms > 0 and past_year_gms < 10 then 10
when past_year_gms >= 10 then round(past_year_gms, (length(cast(round(past_year_gms) as string))-1)*-1)
else 0 end as past_year_gms_rd,
sum(coalesce(chargeable_gms,0)) as chargeable_gms,
sum(coalesce(chargeable_orders,0)) as chargeable_orders,
sum(coalesce(ad_revenue,0)) as ad_revenue,
sum(coalesce(comm_rev,0)) as comm_rev,
sum(coalesce(prolist_cost,0)) as prolist_cost,
sum(coalesce(gms_net,0)) as gms_net,
sum(coalesce(orders,0)) as orders,
sum(coalesce(inc_gms_net,0)) as inc_gms_net,
sum(coalesce(inc_orders,0)) as inc_orders
FROM rev_hist r
left join tier t on r.seller_user_id = t.seller_user_id and t.date = '2023-06-01'
group by 1,2,3,4);


select *
from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist`
limit 100;

END;
