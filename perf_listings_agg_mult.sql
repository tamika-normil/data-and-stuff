with latency as 
    (select date, 

  safe_divide( sum(attributed_attr_rev_adjusted_mult) , sum(attributed_attr_rev) ) as rev_latency_multiplier,
  safe_divide( sum(attributed_gms_adjusted_mult) , sum(attributed_gms) ) as gms_latency_multiplier

  from etsy-data-warehouse-prod.buyatt_rollups.channel_overview
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using (top_channel,second_channel,third_channel,utm_campaign, utm_medium)
  where reporting_channel_group = 'PLA'
  and engine = 'Google - Paid'
  and date >= current_date - 14
  group by 1),
all_listings as 
  (SELECT listing_id, date, sum(visits) as visits,  sum(attr_gms) as attr_gms, sum(attr_rev) as attr_rev,
  sum(cost) as cost
  FROM `etsy-data-warehouse-prod.rollups.perf_listings_agg` 
  where date >= current_date - 14
  and reporting_channel_group = 'PLA'
  and engine = 'Google - Paid'
  group by 1,2)
select date, listing_id,sum(attr_gms) as att_gms,  sum(attr_rev) as att_rev, sum(attr_gms * coalesce(l.gms_latency_multiplier,1)) as attr_gms_adjusted_mult, sum(attr_rev * coalesce(l.rev_latency_multiplier,1)) as attr_rev_adjusted_mult,
from all_listings a
left join latency l using (date)
where date >= current_date - 2
group by 1,2;
