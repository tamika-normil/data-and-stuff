with widget_visits as    
    (select distinct _date as date, visit_id
    from `etsy-data-warehouse-prod.weblog.events`
    where _date>=(current_date-30)
    and run_date>=unix_seconds(timestamp(current_date-30))
    -- -- for backfill
    -- _date>='2021-10-25'
    -- and run_date>=unix_seconds('2021-10-25')
    and event_type in ('dynamic_widget_osa_affiliate_impression', 'widget_osa_affiliate_impression')),
widget_perf as 
    (select date, count(visit_id) as visits, sum(external_source_decay_all) as attr_receipts, sum(external_source_decay_all * gms) as attr_gms,
    sum(case when buyer_type = 'new' then external_source_decay_all * gms end) as attr_gms_new,
    sum(case when buyer_type not in ('new','existing')then external_source_decay_all * gms end) as attr_gms_lapsed,
    from widget_visits w
    left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on w.visit_id = ab.o_visit_id
    group by 1),
osa_visits as 
  (select date(timestamp_seconds(a.click_date)) as date, count(distinct visit_id) as visits, count(distinct awin_publisher_id) active_pubs
  from `etsy-data-warehouse-prod.rollups.osa_click_to_visit_join` o
  join `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks` a using (click_id)
  left JOIN `etsy-data-warehouse-prod.etsy_shard.offsite_ads_affiliate_widget_widget` `w` ON `a`.`widget_id` = `w`.`widget_id`
  where channel = 6 and a.widget_id <> 0 and a.widget_id is not null
  and date(timestamp_seconds(a.click_date)) >= '2022-11-01'
  group by 1),
osa_rev as 
  (select date(timestamp_seconds(a.click_date)) as date, sum(acquisition_fee_usd/100) as osa_rev
  from `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` r
  left join  `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks` a using (click_id)
  where a.channel = 6 and widget_id <> 0 and widget_id is not null
  and date(timestamp_seconds(a.click_date)) >= '2022-11-01'
  group by 1),  
impressions as
    (SELECT date(SAFE.timestamp_millis(TIMESTAMP)) as date, count(event_name) as impressions
    FROM etsy-visit-pipe-prod.canonical.beacons_recent
    WHERE  event_name in ('dynamic_widget_osa_affiliate_impression', 'widget_osa_affiliate_impression')
    and DATE(_PARTITIONTIME) >= "2022-08-01"
    group by 1)
select date, impressions, w.visits as latent_visits, w.attr_receipts, w.attr_gms, w.attr_gms_new, w.attr_gms_lapsed, o.visits as osa_visits, orr.osa_rev, active_pubs, 'affiliates_widget' as channel
from impressions i
full outer join widget_perf w using (date)
full outer join osa_visits o using (date)
full outer join osa_rev orr using (date)
order by 1 desc
;
