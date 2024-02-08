
-- This code is pulled directly from https://looker.etsycloud.com/projects/data_access_controlled/files/ep_processor_fees.b2.bq.view.lkml?line=1

create temp table ep_fees_temp as
   (select
      a.* except(
        payment_gateway
      ,payment_method
      ,card_type
      ,etsy_payment_amount
      ,etsy_payment_amount_usd
      )
      ,case
        when regexp_contains(a.card_type,r'AX') then 'amex'
        when a.instrument_type='K_INSTLMNT' then 'klarna'
        when not regexp_contains(card_type,r'AX') and regexp_contains(a.payment_gateway,r'adyen') then 'adyen'
        when not regexp_contains(card_type,r'AX') and regexp_contains(a.payment_gateway,r'worldpay') then 'worldpay'
        else a.payment_gateway
      end as gateway_w_ax
      ,case
        when regexp_contains(a.payment_gateway,r'adyen') then 'adyen'
        when regexp_contains(a.payment_gateway,r'worldpay') then 'worldpay'
        else a.payment_gateway
      end as payment_gateway
      ,case
        when a.instrument_type = 'GIFTCARD' then 'giftcard'
        else a.payment_method
      end as payment_method
      ,case
        when a.card_type = '' then 'blank'
        else coalesce(a.card_type,'blank')
      end as card_type
      ,etsy_payment_amount
      ,etsy_payment_amount_usd
      ,case
        when is_missing_cost = 0 then etsy_payment_amount_usd
      end as etsy_payment_amount_usd_w_cost
      ,coalesce(proc_aggregated_fees_amount,0) + coalesce(proc_trx_fee_amount,0) as proc_total_fees_amount
      ,coalesce(proc_aggregated_fees_amount_usd,0) + coalesce(proc_trx_fee_amount_usd,0) as proc_total_fees_amount_usd
      ,b.payment_method_variant as pinless_method
    from etsy-data-warehouse-prod.rollups.ep_processor_fees as a
    left join etsy-data-warehouse-prod.rollups.pinless_debit_transactions as b
    on a.cc_txn_id = b.cc_txn_id
    where
      not regexp_contains(lower(processor_mid),r'gift'));

create temp table dev as 
(SELECT date(timestamp_trunc(receipt_timestamp, week)) as week,
sum(transaction_rev) as transaction_rev,
sum(gross_ep_rev) as gross_ep_rev,
sum(ep_rev) as ep_rev,
sum(gross_ep_rev - ep_rev) as ep_fees, 
sum(shipping_rev) as shipping_rev,
sum(listing_rev) as listing_rev,
sum(gms_net) as gms_net,
sum(total_rev) as total_rev
FROM  etsy-data-warehouse-dev.tnormil.receipt_level_take_rate
group by 1
order by 1 desc);

-- FINAL VALIDATION QUERY
-- The take rate calculated by our dev code calculates transaction_rev, net ep_rev or payments rev, sl_rev or shipping label revenue, and listing revenue. 
-- All other revenue types are exclued from our code, and the validation
-- I use all `etsy-data-warehouse-prod.rollups.all_revenue` for all revenue types besides shipping label and listing fees
-- For EP fees, my source of truth assumes the costs reported in go/costdata (i.e. the code above).
-- For listing revenue, my source of truth assumes that total revenue is a combination of revenue from the following: (1) we earn .20 for all orders with a quantity above 1, and (2) we earn .24 for each distinct listing id sold on an order.  
-- For shipping labels, my source of truth assumes the values from `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` 

create temp table prod as 
	(with rev as (
		select date_trunc(date, week) as week, user_id,
			sum(revenue_amount) as total_rev,
			coalesce(sum(case when revenue_stream = "Transaction Revenue" then revenue_amount end), 0) as transaction_rev,
			coalesce(sum(case when revenue_stream = "Etsy Payments Revenue" then revenue_amount end), 0) as ep_rev,
			coalesce(sum(case when revenue_stream = "Shipping Label Revenue" then revenue_amount end), 0) as sl_rev,
		from `etsy-data-warehouse-prod.rollups.all_revenue` 
		where extract(year from date) >= 2023
			and reporting_category != "Minor Revenue"
			and revenue_stream != "Other Services Revenue" 
		group by 1,2
		),
	gms as (
		select date_trunc(trans_date, week) as week, seller_user_id,
			sum(gms_net) as acctg_gms
		from `etsy-data-warehouse-prod.transaction_mart.transactions_gms` 
		where extract(year from acctg_date) >= 2023 
		group by 1,2
		),
	listing_fee_estimates as 
		(select date_trunc(g.date, week) as week, g.seller_user_id, g.receipt_id, sum(g.quantity) as quantity,
		count(distinct listing_id) as listings 
		from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` g 
		join `etsy-data-warehouse-prod.transaction_mart.all_transactions` a on g.transaction_id = a.transaction_id
		where extract(year from g.date) >= 2023
			and g.trans_gms_gross > 0
		group by 1,2,3),	
	listing_fee_estimates_agg as 
		(select week, seller_user_id, count(distinct case when quantity > 1 then receipt_id end) as receipts_quantity_above1,
		sum(listings) as total_num_listings_receipt_level
		from listing_fee_estimates
		group by 1,2),
  shipping_rev as 
    (select date_trunc(order_date, week) as week, seller_user_id, sum(total_revenue) as total_revenue
      from `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` 
      where extract(year from order_date) >= 2023
      group by 1,2)
	select r.week,
		sum(g.acctg_gms) as gms,
		--sum(r.total_rev - osa_rev - ea_rev - r.listing_rev + (receipts_quantity_above1 * .2 + total_num_listings_receipt_level * .24)) as total_rev,
		sum(r.transaction_rev) as transaction_rev,
		sum(r.ep_rev) as ep_rev,
		sum(receipts_quantity_above1 * .2 + total_num_listings_receipt_level * .24) as listing_rev,
		sum(s.total_revenue) as sl_rev,
		--round(sum(r.total_rev - osa_rev - ea_rev - r.listing_rev + (receipts_quantity_above1 * .2 + total_num_listings_receipt_level * .24))/nullif(sum(g.acctg_gms), 0), 4) as take_rate,
	from rev r 
	join gms g 
		on r.user_id = g.seller_user_id and r.week = g.week
	left join listing_fee_estimates_agg lf
		on r.user_id = lf.seller_user_id and r.week = lf.week
  left join shipping_rev s
    on r.user_id = s.seller_user_id and r.week = s.week
group by 1);

with ep_fees as
    (select date_trunc(etsy_trx_date, week) as week, sum(coalesce(proc_aggregated_fees_amount_usd,0) + coalesce(proc_trx_fee_amount_usd,0) ) as ep_fees 
    from ep_fees_temp
    -- this is based on the types tracked in go/costdata
    where (txn_type ) IN ('ADJUSTMENT', 'BILLADJUST', 'BILLING', 'PAYMENT', 'RECOUPMENT')
    group by 1)
select p.week, 
--p.total_rev, 
p.transaction_rev,
p.ep_rev,
ep.ep_fees,
p.sl_rev,
p.listing_rev,
p.gms,
d.total_rev,
d.transaction_rev,
d.gross_ep_rev,
d.ep_rev,
d.ep_fees, 
d.shipping_rev,
d.listing_rev,
d.gms_net
from prod p
left join dev d using (week)
left join ep_fees ep using (week)
order by week desc;
