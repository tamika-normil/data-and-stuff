#what other channel to use as control
#other kpis to check

/*
with pubs_to_select as
    (select distinct publisher_id
    from etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic
    where lower(tactic) = 'social creator co' or publisher_id = '946733'),
receipts as 
    (select distinct date(receipt_timestamp) as date, 
    receipt_id
    from etsy-data-warehouse-prod.buyatt_mart.visits v 
    join pubs_to_select p on v.utm_content  = p.publisher_id
    join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on visit_id = ab.o_visit_id
    and timestamp_seconds(o_visit_run_date) >= '2023-01-01'
    where second_channel = 'affiliates'
    and _date >= '2023-01-01')
select date, case when p.publisher_id is not null and second_channel = 'affiliates' then 'Affiliates Social CC'
else reporting_channel_group end as reporting_channel_group , 
sum(external_source_decay_all) as attr_receipt
from receipts
join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab using (receipt_id)
join  etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using (utm_campaign,
utm_medium,		
top_channel,				
second_channel,
third_channel)
left join pubs_to_select p on v.utm_content  = p.publisher_id
group by 1,2
*/

create or replace table etsy-data-warehouse-dev.tnormil.cc_receipts_visits as 
(with pubs_to_select as
    (select distinct publisher_id
    from etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic
    where lower(tactic) = 'social creator co' or publisher_id = '946733'),
receipts as 
    (select distinct date(receipt_timestamp) as date, 
    receipt_id
    from etsy-data-warehouse-prod.buyatt_mart.visits v 
    join pubs_to_select p on v.utm_content  = p.publisher_id
    join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on visit_id = ab.o_visit_id
    and timestamp_seconds(o_visit_run_date) >= '2023-01-01'
    where second_channel = 'affiliates'
    and _date >= '2023-01-01')
select v.*
from receipts
join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab using (receipt_id)
join  etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd using (utm_campaign,
utm_medium,		
top_channel,				
second_channel,
third_channel));
