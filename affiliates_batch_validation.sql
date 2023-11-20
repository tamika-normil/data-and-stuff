-- owner: marketinganalytics@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: This script supports our batch validation process, and also identifies OSA charged Awin receipts that can be attributed to an affiliate click so we can can pay a higher CPA. 

-- getting visits from ATT opted-in users. only opted-in users will have the ios_advertising_id
create temporary table att_visits as (
select distinct visit_id
from `etsy-data-warehouse-prod.etsy_aux.appsflyer` a
join `etsy-data-warehouse-prod.buyer_growth.native_ids` b on a.ios_advertising_id=b.idfa
and a.att_status=3 and b.event_source='ios'
where b._date>= DATE '2022-11-01'
);

create temporary table osa_transactions as (
select distinct cast(t.order_ref as int64) as Order_Reference, FORMAT_TIMESTAMP('%s', t.transaction_date) as Transaction_Date, 'Amended' as status, 'OSA-Test' as status_note, 
sale_amount_amount, concat('OSA:', sale_amount_amount) as commission_breakdown, sale_amount_currency as currency, t.region
from `etsy-data-warehouse-prod.marketing.awin_spend_data` t
join `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` ar on t.order_ref = cast(ar.receipt_id as string)
join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` ab using (receipt_id)
join `etsy-data-warehouse-prod.buyatt_mart.visits` b on ab.o_visit_run_date = b.run_date and ab.o_visit_id = b.visit_id
left join att_visits att using (visit_id)
LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` r on t.order_ref = cast(r.receipt_id as string)
where ar.channel = 6 
and commission_status in ('pending') 
and receipt_live = 1 #please add line here
and date(transaction_date) >= current_date - 31
and _date>= current_date-61 and o_visit_run_date>= unix_seconds(timestamp(current_date-61)) and run_date>= unix_seconds(timestamp(current_date-61))
and ((mapped_platform_type like 'boe_ios%%' and att.visit_id is not null) or mapped_platform_type not like 'boe_ios%%'));

create temporary table validations as
(SELECT distinct t.receipt_id as Order_Reference,
FORMAT_TIMESTAMP('%s', t.transaction_date) as Transaction_Date,
case when r.receipt_live is null then 'declined'
when r.receipt_live = 0 then 'declined'
when r.receipt_live = 1 then 'accepted' else 'declined' end as Status,
case when r.receipt_live is null then 'Order cancelled'
when r.receipt_live = 0 then 'Order cancelled'
when r.receipt_live = 1 then '' else '' end as Status_Note
FROM `etsy-data-warehouse-prod.marketing.awin_temp_transaction_validation` t
LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_receipts` r using (receipt_id)
LEFT JOIN osa_transactions o on t.receipt_id = o.Order_Reference
left join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` ab on t.receipt_id=ab.receipt_id and o_visit_run_date>= unix_seconds(timestamp(current_date-61))
left join `etsy-data-warehouse-prod.buyatt_mart.visits` b on ab.o_visit_run_date = b.run_date and ab.o_visit_id = b.visit_id 
	and run_date>= unix_seconds(timestamp(current_date-61)) and _date>=current_date-61
left join att_visits att using (visit_id)
where o.Order_Reference is null
and  ((mapped_platform_type like 'boe_ios%%' and att.visit_id is not null) or mapped_platform_type not like 'boe_ios%%')
ORDER BY 2);

select 
	Order_Reference,
	Transaction_Date,
	status,
	status_note,
	sale_amount_amount,
	commission_breakdown,
	currency
from osa_transactions 
where region='%s'
union all
select *, 
null as sale_amount_amount, null as commission_breakdown, null as currency
from validations;
