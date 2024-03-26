-- dev - check for multiple receipt statuses
SELECT commission_status, count(*)
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test` group by 1;

-- dev - check receipt count
-- 2368153
-- there are more receipts in dev than prod
SELECT count(*)
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05');

-- prod - check receipt count
-- 2367783
SELECT count(*)
FROM `etsy-data-warehouse-prod.marketing.awin_spend_data`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05');

-- dev - check for dupe receipts
-- 67 order refs have dupe rows
SELECT order_ref, count(*)
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
group by 1
having count(*) > 1;

-- prod - check for dupe receipts
-- 16 order refs have dupe rows
-- bonuses have an order ref of 0, app sales have an order ref of 0, and some receipts have an approval and decline row 
SELECT order_ref, count(*)
FROM `etsy-data-warehouse-prod.marketing.awin_spend_data`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
group by 1
having count(*) > 1;


-- no missing receipts
-- there are no receipts in prod, not found in dev
with missing_receipts as 
(SELECT order_ref
FROM `etsy-data-warehouse-prod.marketing.awin_spend_data`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
except distinct 
SELECT order_ref
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05'))
select date(transaction_date), count(*)
from  `etsy-data-warehouse-prod.marketing.awin_spend_data`
join missing_receipts using (order_ref)
group by 1
;

-- no missing receipts
-- there are receipts in dev, not found in prod that occured on 2023-10-10
with missing_receipts as 
(SELECT order_ref
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
except distinct
SELECT order_ref
FROM `etsy-data-warehouse-prod.marketing.awin_spend_data`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
)
select *
from `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
join missing_receipts using (order_ref)
;


-- dev - check unexpected dupe receipts 
-- 8 rows
with dupe_receipts as 
(SELECT order_ref, count(*), count(distinct commission_status) as status
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
group by 1
having count(*) > 1)
select *
from `etsy-data-warehouse-dev.marketing.awin_spend_data_test`
join dupe_receipts using (order_ref)
where order_ref <> '0' and status = 1
order by order_ref
;

-- prod - check unexpected dupe receipts 
-- 35 rows
with dupe_receipts as 
(SELECT order_ref, count(*), count(distinct commission_status) as status
FROM `etsy-data-warehouse-prod.marketing.awin_spend_data`
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
group by 1
having count(*) > 1)
select *
from `etsy-data-warehouse-prod.marketing.awin_spend_data`
join dupe_receipts using (order_ref)
where order_ref <> '0' and status = 1
order by order_ref
;

(SELECT date(transaction_date) as date, sum(sale_amount_amount) as sale_amount_amount, sum(commission_amount_amount) as commission_amount_amount,
safe_divide(sum(commission_amount_amount), sum(sale_amount_amount)) as cpa
FROM `etsy-data-warehouse-dev.marketing.awin_spend_data_test` a
join etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts r on a.order_ref = cast(r.receipt_id as string) and r.channel = 6
where date(transaction_date) >= date('2023-12-04')
and date(transaction_date) <= date('2024-02-05')
group by 1
order by 1)
