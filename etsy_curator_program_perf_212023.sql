
select sum(external_source_decay_all * gms) as gms,sum(paid_last_click_all * gms) as paid_gms
from etsy-data-warehouse-prod.buyatt_mart.visits v
left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id 
where utm_content = '946733'
and second_channel = 'affiliates';

select sum(external_source_decay_all * gms) as gms,sum(paid_last_click_all * gms) as paid_gms
from etsy-data-warehouse-prod.buyatt_mart.visits v
left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id 
where start_datetime between '2022-10-01' and '2022-12-31'
and second_channel = 'affiliates';

with ciq_receipts as
(select distinct receipt_id
from etsy-data-warehouse-prod.buyatt_mart.visits v
left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id 
where utm_content = '946733'
and second_channel = 'affiliates'),
receipts_channel as 
(select reporting_channel_group, sum(external_source_decay_all * gms) as gms
from ciq_receipts r
join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on r.receipt_id = ab.receipt_id
join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd using (top_channel, second_channel, third_channel, utm_campaign, utm_medium)
group by 1)
select *
from receipts_channel;

with ciq_receipts as
(select distinct receipt_id
from etsy-data-warehouse-prod.buyatt_mart.visits v
left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id 
where second_channel = 'affiliates'
and start_datetime between '2022-10-01' and '2022-12-31'),
receipts_channel as 
(select reporting_channel_group, sum(external_source_decay_all * gms) as gms
from ciq_receipts r
join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on r.receipt_id = ab.receipt_id
join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd using (top_channel, second_channel, third_channel, utm_campaign, utm_medium)
group by 1)
select *
from receipts_channel;
