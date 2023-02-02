with awim_orders as 
  (select date(transaction_date) as date, publisher_id, count(distinct order_ref) as awin_orders
  from etsy-data-warehouse-prod.marketing.awin_spend_data
  WHERE commission_status in ('pending','approved')
  group by 1,2)
 select day, date(date_trunc(day,month)) as month,  date(date_trunc(day,week)) as week, split(account_name,' ')[SAFE_OFFSET(0)] as pub, sum(attributed_rev_purch_date) as attributed_rev_purch_date,
 sum(attributed_rev_purch_date * coalesce(safe_divide(attr_gms_total_new,attr_gms_total_old),1)) as attributed_rev_purch_date_mult,
 sum(a.cost) as costs, sum(visits) as visits, sum(attr_receipts_purch_date  * coalesce(safe_divide(attr_gms_total_new,attr_gms_total_old),1)) as attr_receipts_purch_date_mult, 
 sum(attr_receipts_purch_date) as attr_receipts_purch_date,
 sum(awin_orders) as awin_orders
 from `etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker` a
 left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` b on split(a.account_name,' ')[SAFE_OFFSET(0)] = b.publisher_id and a.engine = 'affiliate'
 left join etsy-data-warehouse-prod.rollups.awin_content_attribution_click c on split(a.account_name,' ')[SAFE_OFFSET(0)] = c.utm_content 
 and date_trunc(a.day,month) = c.month
 left join awim_orders aw on split(a.account_name,' ')[SAFE_OFFSET(0)] = cast(aw.publisher_id as string) and date(a.day) = aw.date
where engine = 'affiliate' 
and split(account_name,' ')[SAFE_OFFSET(0)] in ('218047',
'228531')
and day >= '2022-04-01'
group by 1,2,3,4;


with ciq_receipts as
(select distinct receipt_id, utm_content 
from etsy-data-warehouse-prod.buyatt_mart.visits v
left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id 
where utm_content in ('218047',
'228531')
and second_channel = 'affiliates'
and date(start_datetime) >= '2022-03-01'),
receipts_channel as 
(select reporting_channel_group, case when second_channel = 'affiliates' then 'affiliates' when top_channel in ('us_paid', 'intl_paid') and second_channel <> 'affiliates'then 'paid' else 'nonpaid' end as type, date(receipt_timestamp) as date, r.utm_content, sum(external_source_decay_all * gms) as gms
from ciq_receipts r
join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on r.receipt_id = ab.receipt_id
join etsy-data-warehouse-prod.buyatt_mart.visits v on ab.o_visit_id = v.visit_id
left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd using (top_channel, second_channel, third_channel, utm_campaign, utm_medium)
group by 1,2,3,4)
select *
from receipts_channel;
