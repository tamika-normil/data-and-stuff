#cvr/aov assumptions
select date_trunc(start_datetime,month) as date, case when v.user_id is not null and v.user_id <> 0 then 1 else 0 end as signed_in,
count(distinct visit_id) as visits, sum(external_source_decay_all) as attr_receipt,  sum(case when r.receipt_id is not null then external_source_decay_all end) as chargeable_attr_receipt, 
  sum(case when r.receipt_id is not null then external_source_decay_all * ab.gms end) as chargeable_attr_gms, sum(external_source_decay_all * ab.gms) as attr_gms
, sum(external_source_decay_all * attr_rev) as attr_rev
from (select distinct visit_id, click_date from etsy-data-warehouse-prod.rollups.osa_click_to_visit_join) o
 left join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
 left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id
 left join etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv clv on ab.receipt_id = clv.receipt_id
left join etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts r on clv.receipt_id = r.receipt_id and r.status = 1
where o.click_date >= '2022-10-01'
and _date > '2022-10-01'
group by 1, 2;

#buyers who return assumptions
with all_clicks as 
(select v.user_id, v.browser_id, v.platform, visit_id, start_datetime, lead(start_datetime)
    OVER (PARTITION BY v.browser_id ORDER BY start_datetime ASC) AS next_start_datetime, 
lead(visit_id)
    OVER (PARTITION BY v.browser_id ORDER BY start_datetime ASC) AS next_visit_id,     
case when r.receipt_id is not null then 1 else 0 end as osa_charged,
case when v.user_id is not null and v.user_id <> 0 then 1 else 0 end as signed_in,
external_source_decay_all 
from (select distinct visit_id, click_date from etsy-data-warehouse-prod.rollups.osa_click_to_visit_join) o
 left join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
 left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id
 left join etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts r using (receipt_id)
where o.click_date >= '2022-10-01'
and _date > '2022-10-01')
select date(start_datetime) as date, count(browser_id) as browsers, count(case when date_diff(start_datetime, next_start_datetime, day) < 30 then browser_id end) as browsers_return,
 count(visit_id) as visits, count(case when date_diff(start_datetime, next_start_datetime, day) < 30 then visit_id end) as visits_return,
 count(case when signed_in = 1 then visit_id end) as visits_signed_in, count(case when date_diff(start_datetime, next_start_datetime, day) < 30 and signed_in = 1 then visit_id end) as visits_return_signed_in,
 sum(case when osa_charged = 1 then external_source_decay_all end) as att_receipt
from all_clicks 
group by 1
order by 1 desc;

#time between visit and next visit 
with all_clicks as 
(select v.user_id, v.browser_id, v.platform, visit_id, start_datetime, lead(start_datetime)
    OVER (PARTITION BY v.browser_id ORDER BY start_datetime ASC) AS next_start_datetime, 
lead(visit_id)
    OVER (PARTITION BY v.browser_id ORDER BY start_datetime ASC) AS next_visit_id,     
case when r.receipt_id is not null then 1 else 0 end as osa_charged,
case when v.user_id is not null and v.user_id <> 0 then 1 else 0 end as signed_in,
external_source_decay_all 
from (select distinct visit_id, click_date from etsy-data-warehouse-prod.rollups.osa_click_to_visit_join) o
 left join etsy-data-warehouse-prod.weblog.visits v using (visit_id)
 left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id
 left join etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts r using (receipt_id)
where o.click_date >= '2022-10-01'
and _date > '2022-10-01')
select date_trunc(start_datetime,month) as date, date_diff(next_start_datetime, start_datetime, hour) as hours_between_first_last, count(distinct visit_id) as visits
from all_clicks 
where signed_in =  1
and next_start_datetime is not null
group by 1,2
order by 1 desc;
