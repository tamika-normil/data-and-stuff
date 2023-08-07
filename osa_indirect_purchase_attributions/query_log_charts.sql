-- indirect gms by attribution windoe
select attribution_window_number_of_day,
sum(inc_gms_net) as gms_net,
sum(ad_revenue) as ad_revenue,
count(distinct seller_user_id) as sellers
from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
where seller_tier not in ('active seller',
'non active seller')
group by 1
order by 1;

-- sellers w/ indirect GMS who were never charged before
select attribution_window_number_of_day,
case when ad_revenue <= 0 or ad_revenue is null then 0 else 1 end as charged_before,
case when seller_tier in ('seller with a sale','high potential seller') then 'seller with a sale/high potential seller' else seller_tier end as seller_tier,
count(distinct seller_user_id) as sellers
from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
where seller_tier not in ('active seller',
'non active seller')
and inc_gms_net > 0 
group by 1,2,3
order by 1;


select case when past_year_gms_rd < 1000 then 1000 else past_year_gms_rd end as past_year_gms_rd, case when ad_revenue > 0 then 1 else 0 end as charged_before, 
count(distinct case when inc_gms_net > 0 then seller_user_id end) as sellers
from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist`
group by 1,2
order by 1;

-- revenue history by past year gms 
select case when past_year_gms_rd >= 500000 then 500000 else past_year_gms_rd end as past_year_gms_rd, 
sum(chargeable_gms) as chargeable_gms,
sum(ad_revenue) as ad_revenue,
sum(comm_rev) as comm_rev,
sum(prolist_cost) as prolist_cost,
sum(gms_net) as gms_net,
sum(inc_gms_net) as inc_gms_net,
count(distinct seller_user_id) as sellers,
sum(orders) as orders,
from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
where seller_tier not in ('active seller',
'non active seller')
and i.attribution_window_number_of_day = 1
group by 1
order by 1;

-- distribution of sellers by tenure and take rate
with sum_data_seller as 
    (select seller_user_id, 
    attribution_window_number_of_day,
    seller_tier,
    date_diff(LAST_DAY(DATE '2023-06-01', MONTH), open_date, month) as months_since_open_avg,
    min(sb.past_year_gms) as min_past_year_gms,
    max(sb.past_year_gms) as max_past_year_gms,
    sum(chargeable_gms) as chargeable_gms,
    sum(ad_revenue) as ad_revenue,
    sum(comm_rev) as comm_rev,
    sum(prolist_cost) as prolist_cost,
    sum(gms_net) as gms_net,
    sum(inc_gms_net) as inc_gms_net,
    from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
    left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
    where seller_tier not in ('active seller',
    'non active seller')
    group by 1,2,3,4),
take_rates as 
  (select *,
  round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) as bau_take_rate,
  case when round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) >= 50 then 50
  when round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) < 50 and round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) >= 40 then 40
  when round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) < 40 and round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) >= 30 then 30
  when round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) < 30 and round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) >= 20 then 20
  when round(safe_divide(ad_revenue+comm_rev+prolist_cost, gms_net)*100,1) < 20 then 0 end as take_rate_bin
  from sum_data_seller)
select 
take_rate_bin,
case when months_since_open_avg >= 72 then 72 else months_since_open_avg end as months_since_open_avg ,
count(distinct seller_user_id) as sellers
from take_rates
group by 1,2;

-- revenue history by seller tier
select seller_tier, 
attribution_window_number_of_day,
min(sb.past_year_gms) as min_past_year_gms,
max(sb.past_year_gms) as max_past_year_gms,
sum(chargeable_gms) as chargeable_gms,
sum(ad_revenue) as ad_revenue,
sum(comm_rev) as comm_rev,
sum(prolist_cost) as prolist_cost,
sum(gms_net) as gms_net,
sum(inc_gms_net) as inc_gms_net,
count(distinct seller_user_id) as sellers
from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
group by 1,2
order by 1;

/*
with take_rates_seller as 
    (select seller_user_id, 
    attribution_window_number_of_day,
    seller_tier,
    min(sb.past_year_gms) as min_past_year_gms,
    max(sb.past_year_gms) as max_past_year_gms,
    sum(chargeable_gms) as chargeable_gms,
    sum(ad_revenue) as ad_revenue,
    sum(comm_rev) as comm_rev,
    sum(prolist_cost) as prolist_cost,
    sum(gms_net) as gms_net,
    sum(inc_gms_net) as inc_gms_net,
    from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
    left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
    where seller_tier not in ('active seller',
    'non active seller')
    group by 1,2,3)
select attribution_window_number_of_day,
seller_tier,
case when round(safe_divide(ad_revenue, gms_net)*100,1) < 10 then round(safe_divide(ad_revenue, gms_net)*100,1) else 10 end as bau_take_rate,
case when round(safe_divide(ad_revenue+(inc_gms_net*.01),gms_net)*100,1) < 10 then round(safe_divide(ad_revenue+(inc_gms_net*.01),gms_net)*100,1) else 10 end as new_take_rate,
count(distinct seller_user_id) as sellers
from take_rates_seller
group by 1,2,3,4;
*/

-- fee distribution by seller tier those have have been charged/not charged before 
with take_rates_seller as 
    (select seller_user_id, 
    attribution_window_number_of_day,
    seller_tier,
    min(sb.past_year_gms) as min_past_year_gms,
    max(sb.past_year_gms) as max_past_year_gms,
    sum(chargeable_gms) as chargeable_gms,
    sum(chargeable_orders) as chargeable_orders,
    sum(ad_revenue) as ad_revenue,
    sum(comm_rev) as comm_rev,
    sum(prolist_cost) as prolist_cost,
    sum(gms_net) as gms_net,
    sum(inc_gms_net) as inc_gms_net,
    sum(inc_orders) as inc_orders
    from `etsy-data-warehouse-dev.tnormil.indirect_w_rev_hist` i
    left join etsy-data-warehouse-prod.rollups.seller_basics_all sb on sb.user_id = i.seller_user_id
    where seller_tier not in ('active seller',
    'non active seller')
    group by 1,2,3)
select 
case when ad_revenue <= 0 or ad_revenue is null then 0 else 1 end as charged_before,
case when round(inc_gms_net*.01) < 10 then round(inc_gms_net*.01)
when round(inc_gms_net*.01) >= 10 then round(inc_gms_net*.01, (length(cast(round(inc_gms_net*.01) as string))-1)*-1)
else 0 end as yearly_fee,
case when round(inc_gms_net) < 10 then 0
when round(inc_gms_net) >= 10 then round(inc_gms_net, (length(cast(round(inc_gms_net) as string))-1)*-1)
else 0 end as yearly_inc_gms,
attribution_window_number_of_day,
seller_tier,
count(distinct seller_user_id) as sellers
from take_rates_seller
where inc_gms_net > 0
group by 1,2,3,4,5;

-- historical ad revenue
(select date_trunc(date(timestamp_seconds(purchase_date)), month) as date, 
  sum(gms_usd/100) as chargeable_gms,
  sum(acquisition_fee_usd/100) as ad_revenue,
from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts a
left join etsy-data-warehouse-prod.rollups.seller_basics_all s using (shop_id)
left join `etsy-data-warehouse-prod.transaction_mart.all_receipts` r using (receipt_id)
where receipt_live = 1
group by 1)
