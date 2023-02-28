begin

create temp table opt_out_data as
(with optouts as 
  (SELECT opt_out_date, shop_id, becomes_required_date, seller_required
  FROM `etsy-data-warehouse-prod.rollups.offsite_ads_marketing`
  where opt_out_date is not null),
perf_listings as 
  (select ar.shop_id, date(timestamp_seconds(ar.purchase_date)) as date, opt_out_date, sum(acquisition_fee_usd / 100) as attr_gms, sum(case when seller_required = 1 then acquisition_fee_usd / 100 end) as attr_gms_become_required
  from etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts ar
  join optouts o using (shop_id)
  group by 1,2,3),
dates as 
  (SELECT shop_id, date
  FROM  (select shop_id, opt_out_date, min(date) as first_sale from perf_listings group by 1,2)
  join UNNEST(GENERATE_DATE_ARRAY(first_sale, opt_out_date)) AS date)
select date, count(o.shop_id) as shops, sum(last_yr_attr_gms) as last_yr_attr_gms, sum(last_yr_attr_gms_required) as last_yr_attr_gms_required
from optouts o
left join (select shop_id, date, attr_gms, sum(coalesce(attr_gms,0)) OVER (
    PARTITION BY SHOP_ID
    ORDER BY unix_seconds(timestamp(date)) desc
    rows BETWEEN 365 PRECEDING AND CURRENT ROW) as last_yr_attr_gms 
    , sum(coalesce(attr_gms_become_required,0)) OVER (
    PARTITION BY SHOP_ID
    ORDER BY unix_seconds(timestamp(date)) desc
    rows BETWEEN 365 PRECEDING AND CURRENT ROW) as last_yr_attr_gms_required 
    from dates
    left join perf_listings using (date, shop_id)) p on opt_out_date = date and o.shop_id = p.shop_id
    group by 1
  order by 1 desc);

with optins as
  (SELECT date, s.shop_id, opt_in_date
  FROM  `etsy-data-warehouse-prod.rollups.offsite_ads_marketing` s
  join UNNEST(GENERATE_DATE_ARRAY(s.opt_in_date, current_date - 1, INTERVAL 1 DAY)) AS date
  where (opt_out_date > date or opt_out_date is null) and opt_in_date is not null) ,
optins_agg as 
  (select date, count(*) as shops
  from optins
  group by 1)
select *
from optins_agg a
left join opt_out_data b on a.date = b.date;

end 
