#influnecer coupon usage

create or replace temporary table coupons as ( 
select 
  campaign_id, 
  audience_name,
  case when promotion_mechanism = 'fixed' then concat('$',round(promotion_amount/100,0))
       when promotion_mechanism = 'percentage' then concat(promotion_amount,'%')
       else cast(promotion_amount as string) end as promotion_amount,
  promotion_type, --coupon or giftcard 
  promotion_mechanism,--fixed or percentage off 
  expiration_days, 
  timestamp_seconds(create_date) as create_date, 
  timestamp_seconds(update_date) as update_date, 
  currency_code,
  audience_description,
  created_by, 
  readable_code as coupon_code,
  round(minimum_spend_amount/100,0) as min_spend, 
  round(maximum_discount_amount/100,0) as max_discount,
  timestamp_seconds(expiration_date) as expiration_date, 
  eligible_platform, --web, boe or null
  buyer_type --new /existing 
from 
  `etsy-data-warehouse-prod.etsy_aux.discount_campaign_configs`
where 
  ((lower(audience_name) not like '%tirekick%' and 
  audience_name not in ('coupon_admin_launch','testconfig')) --cleaning up some test coupons
  or audience_name is null) and
  promotion_type = 'coupon'
);

create temp table collection_receipts as 
(select distinct ep_page_title,v.receipt_id
from `etsy-data-warehouse-dev.tnormil.collection_visits` e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` v on e.visit_id = v.o_visit_id
join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` av using (receipt_id)
where type in ('influencer marketing', 'influencer affiliate marketing') and get_valid_gms = 1
union distinct
select distinct ep_page_title,tv.receipt_id
from `etsy-data-warehouse-dev.tnormil.collection_visits` e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv using (visit_id)
join `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` gt using (transaction_id)
where type not in ('influencer marketing', 'influencer affiliate marketing') and get_valid_gms = 1);


select ep_page_title, v.top_channel, 
d.coupon_code, 
d.audience_name, 
d.promotion_amount, 
d.buyer_type,
count(cr.receipt_id) as receipts
from 
  coupons d 
left join 
  `etsy-data-warehouse-prod.etsy_shard.discount_user_promotion_mappings` m on d.campaign_id = m.campaign_id
left join 
  `etsy-data-warehouse-prod.etsy_shard.etsy_coupons` c on m.promotion_id = c.coupon_id 
left join 
  `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on c.receipt_group_id = r.receipt_group_id and r.receipt_live = 1 
left join 
  `etsy-data-warehouse-prod.etsy_shard.shop_receipts_totals` t on r.receipt_id = t.receipt_id
left join 
  `etsy-data-warehouse-prod.transaction_mart.receipts_gms` g on r.receipt_id = g.receipt_id  
left join collection_receipts cr on r.receipt_id = cr.receipt_id
left join 
  (select distinct 
    receipt_id, 
    platform_app, 
    top_channel, 
    start_datetime
    from 
      `etsy-data-warehouse-prod.transaction_mart.transactions_visits`
   ) v on g.receipt_id = v.receipt_id and v.start_datetime >= d.create_date
left join 
  (select 
    coupon_code, 
    sum(total_attempts) as total_attempts, 
    sum(total_unique_attempts) as total_unique_attempts
  from `etsy-data-warehouse-prod.rollups.coupon_attempts_daily`
  group by 1) a on d.coupon_code = a.coupon_code
where cr.receipt_id is not null and ep_page_title in ('gq-favorites', 'gifting-roundup')
group by 1,2,3,4,5,6;

 
/*
left join 
  (select distinct 
    receipt_id, 
    platform_app, 
    top_channel, 
    start_datetime
    from 
      `etsy-data-warehouse-prod.transaction_mart.transactions_visits`
   ) v on g.receipt_id = v.receipt_id and v.start_datetime >= d.create_date
left join 
  (select 
    coupon_code, 
    sum(total_attempts) as total_attempts, 
    sum(total_unique_attempts) as total_unique_attempts
  from `etsy-data-warehouse-prod.rollups.coupon_attempts_daily`
  group by 1) a on d.coupon_code = a.coupon_code
  */
  
  
  #influnecer coupon usage

create or replace temporary table coupons as ( 
select 
  campaign_id, 
  audience_name,
  case when promotion_mechanism = 'fixed' then concat('$',round(promotion_amount/100,0))
       when promotion_mechanism = 'percentage' then concat(promotion_amount,'%')
       else cast(promotion_amount as string) end as promotion_amount,
  promotion_type, --coupon or giftcard 
  promotion_mechanism,--fixed or percentage off 
  expiration_days, 
  timestamp_seconds(create_date) as create_date, 
  timestamp_seconds(update_date) as update_date, 
  currency_code,
  audience_description,
  created_by, 
  readable_code as coupon_code,
  round(minimum_spend_amount/100,0) as min_spend, 
  round(maximum_discount_amount/100,0) as max_discount,
  timestamp_seconds(expiration_date) as expiration_date, 
  eligible_platform, --web, boe or null
  buyer_type --new /existing 
from 
  `etsy-data-warehouse-prod.etsy_aux.discount_campaign_configs`
where 
  ((lower(audience_name) not like '%tirekick%' and 
  audience_name not in ('coupon_admin_launch','testconfig')) --cleaning up some test coupons
  or audience_name is null) and
  promotion_type = 'coupon'
);

create temp table collection_receipts as 
(select distinct ep_page_title,v.receipt_id
from `etsy-data-warehouse-dev.tnormil.collection_visits` e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` v on e.visit_id = v.o_visit_id
join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` av using (receipt_id)
where type in ('influencer marketing', 'influencer affiliate marketing') and get_valid_gms = 1
union distinct
select distinct ep_page_title,tv.receipt_id
from `etsy-data-warehouse-dev.tnormil.collection_visits` e
-----------------
-- FLAG REF ---- 
-----------------
join `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv using (visit_id)
join `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` gt using (transaction_id)
where type not in ('influencer marketing', 'influencer affiliate marketing') and get_valid_gms = 1);


select ep_page_title, v.top_channel, 
d.coupon_code, 
d.audience_name, 
d.promotion_amount, 
d.buyer_type,
count(cr.receipt_id) as receipts
from 
  coupons d 
left join 
  `etsy-data-warehouse-prod.etsy_shard.discount_user_promotion_mappings` m on d.campaign_id = m.campaign_id
left join 
  `etsy-data-warehouse-prod.etsy_shard.etsy_coupons` c on m.promotion_id = c.coupon_id 
left join 
  `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on c.receipt_group_id = r.receipt_group_id and r.receipt_live = 1 
left join 
  `etsy-data-warehouse-prod.etsy_shard.shop_receipts_totals` t on r.receipt_id = t.receipt_id
left join 
  `etsy-data-warehouse-prod.transaction_mart.receipts_gms` g on r.receipt_id = g.receipt_id  
left join collection_receipts cr on r.receipt_id = cr.receipt_id
left join 
  (select distinct 
    receipt_id, 
    platform_app, 
    top_channel, 
    start_datetime
    from 
      `etsy-data-warehouse-prod.transaction_mart.transactions_visits`
   ) v on g.receipt_id = v.receipt_id and v.start_datetime >= d.create_date
left join 
  (select 
    coupon_code, 
    sum(total_attempts) as total_attempts, 
    sum(total_unique_attempts) as total_unique_attempts
  from `etsy-data-warehouse-prod.rollups.coupon_attempts_daily`
  group by 1) a on d.coupon_code = a.coupon_code
where cr.receipt_id is not null and ep_page_title in ('gq-favorites', 'gifting-roundup')
group by 1,2,3,4,5,6;

 
/*
left join 
  (select distinct 
    receipt_id, 
    platform_app, 
    top_channel, 
    start_datetime
    from 
      `etsy-data-warehouse-prod.transaction_mart.transactions_visits`
   ) v on g.receipt_id = v.receipt_id and v.start_datetime >= d.create_date
left join 
  (select 
    coupon_code, 
    sum(total_attempts) as total_attempts, 
    sum(total_unique_attempts) as total_unique_attempts
  from `etsy-data-warehouse-prod.rollups.coupon_attempts_daily`
  group by 1) a on d.coupon_code = a.coupon_code
  */
