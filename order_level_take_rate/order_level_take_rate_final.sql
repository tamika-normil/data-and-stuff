begin

/*
HIGH LEVEL GOAL 

The goal of this script is to estimate receipt level total take rate and the take rate componenets

The take rate componenets include transaction_rev, net ep_rev or payments rev, sl_rev or shipping label revenue, and listing revenue. 
*/

/*
STEP 1

Get actualized receipt level transaction revenue, and GROSS ep payments revenue from orders in the past 90 days. 

GROSS ep payments revenue does NOT account for additional fees we pay to vendors and bad debt / Cost of Refunds. GROSS ep payments revenue accounts for additional revenue we earn from 
currency exchange. 
*/

create temp table commission_ep_rev as 
    (with txn_gms as (
      select transaction_id,
        receipt_id,
        gms_net,
        seller_country_name as seller_country,
        buyer_country_name as buyer_country 
      from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` 
      where date >= current_date - 90
        and date < current_date
      ),
    txn_rev as (
      select type_id as transaction_id,
        sum(amount_origin_ccy) as transaction_revenue
      from `etsy-data-warehouse-prod.etsy_shard.financially_relevant_event_ledger_entry_insert` 
      where ledger_entry_insert_date >= unix_seconds(timestamp(current_date - 90))
        and type in ("transaction","transaction_refund")
      group by 1
      ),
    txn_shipping_rev as (
      select type_id as receipt_id,
        sum(-amount_origin_ccy) as transaction_shipping_revenue
      from `etsy-data-warehouse-prod.etsy_shard.financially_relevant_event_ledger_entry_insert` 
      where ledger_entry_insert_date >= unix_seconds(timestamp(current_date - 90))
        and type in ("shipping_transaction","shipping_transaction_refund")
      group by 1
      ),
    receipt_gms_rev as (
      select g.receipt_id,
        max(g.seller_country) as seller_country,
        max(g.buyer_country) as buyer_country,
        sum(g.gms_net) as gms,
        coalesce(sum(-r.transaction_revenue), 0) as transaction_revenue
      from txn_gms g 
      left join txn_rev r 
        on g.transaction_id = r.transaction_id
      group by 1
      ),
    ep_rev as (
      select type_id as shop_payment_id,
        shop_id,
        sum(-amount_origin_ccy) as ep_revenue
      from `etsy-data-warehouse-prod.etsy_shard.financially_relevant_event_ledger_entry_insert` 
      where ledger_entry_insert_date >= unix_seconds(timestamp(current_date - 90))
        and type in ("PAYMENT_PROCESSING_FEE","REFUND_PROCESSING_FEE")
      group by 1,2
      ),
    fx_hedge as (
      select receipt_id,
        coalesce(sum(net_markdown_hedge_ledger_usd), 0) as markdown_revenue,
        coalesce(sum(net_markup_hedge_buyer_usd), 0) as markup_revenue 
      from `etsy-data-warehouse-prod.accounting_sl.fre_etsy_payments_detail`
      where date >= current_date - 90
        and date < current_date 
      group by 1
      ),
    payment_ids as (
      select receipt_id,
        shop_payment_id,
        count(*) as records 
      from `etsy-data-warehouse-prod.etsy_payments.shop_payments` 
      where create_date >= unix_seconds(timestamp(current_date - 90))
      group by 1,2
      )
select r.receipt_id,
  r.gms, 
  r.seller_country,
  r.buyer_country,
  case when r.seller_country in ("United States","United Kingdom","Canada","Germany","France","Australia") then r.seller_country else "Rest of world" end as seller_country_grouped,
      case when r.buyer_country in ("United States","United Kingdom","Canada","Germany","France","Australia") then r.buyer_country else "Rest of world" end as buyer_country_grouped,
  r.transaction_revenue,
  coalesce(ts.transaction_shipping_revenue, 0) as transaction_shipping_revenue,
  coalesce(ep.ep_revenue, 0) as ep_revenue,
  coalesce(fx.markdown_revenue + fx.markup_revenue, 0) as fx_hedge_revenue,
  round((r.transaction_revenue + coalesce(ts.transaction_shipping_revenue, 0)) / nullif(r.gms, 0), 6) as transaction_take_rate,
  round(coalesce(ep.ep_revenue, 0) / nullif(r.gms, 0), 6) as ep_take_rate,
  round(coalesce(fx.markdown_revenue + fx.markup_revenue, 0) / nullif(r.gms, 0), 6) as fx_hedge_take_rate,
  round((coalesce(ep.ep_revenue, 0) + coalesce(fx.markdown_revenue + fx.markup_revenue, 0)) / nullif(r.gms, 0), 6) as total_ep_take_rate
from receipt_gms_rev r 
left join txn_shipping_rev ts 
  on r.receipt_id = ts.receipt_id
left join payment_ids s 
  on r.receipt_id = s.receipt_id
left join ep_rev ep 
  on s.shop_payment_id = ep.shop_payment_id
left join fx_hedge fx 
  on r.receipt_id = fx.receipt_id
where r.gms > 0
    );

/*
create temp table shipping_rev_by_trade_route as 
      (select case when seller_country in ("United States","United Kingdom","Canada","Germany","France","Australia") then seller_country else "Rest of world" end as seller_country_grouped,
      case when buyer_country in ("United States","United Kingdom","Canada","Germany","France","Australia") then buyer_country else "Rest of world" end as buyer_country_grouped,
      round(avg(total_revenue), 4) as shipping_rev_per_order 
      from `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` 
      where order_date >= current_date - 365 
      group by 1,2);
*/

/*
STEP 1

Estimate shipping label revenue. We have actualized shipping label revenue data in  `etsy-data-warehouse-prod.rollups.receipt_shipping_basics`. 

*/

create temp table shipping_rev_by_trade_route as (
    with shipping_routes as 
    (select distinct order_date, case when seller_country in ("United States","United Kingdom","Canada","Germany","France","Australia") then seller_country else "Rest of world" end as seller_country_grouped,
    case when buyer_country in ("United States","United Kingdom","Canada","Germany","France","Australia") then buyer_country else "Rest of world" end as buyer_country_grouped,
    avg(total_revenue) as total_revenue
    from `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` 
    where order_date >= '2021-01-01'
    group by 1,2,3)
    select distinct order_date, seller_country_grouped, buyer_country_grouped,
      avg(total_revenue) 
  OVER (
      PARTITION BY seller_country_grouped, buyer_country_grouped
      ORDER BY order_date desc
      ROWS BETWEEN 365 PRECEDING AND CURRENT ROW) as avg_shipping_revenue_trailing_year
    from shipping_routes
    where order_date >= current_date - 90);

create temp table listing_fees as (
with listing_fee_estimates as 
		(select g.receipt_id, sum(g.quantity) as quantity,
		count(distinct listing_id) as listings 
		from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` g 
		join `etsy-data-warehouse-prod.transaction_mart.all_transactions` a on g.transaction_id = a.transaction_id
		where g.date >= current_date - 90
    group by 1) 
		select receipt_id, (count(distinct case when quantity > 1 then receipt_id end) * .2  + 
		sum(listings) * .24) as listing_fees
		from listing_fee_estimates
		group by 1);	
  
create temp table receipt_ep_fees as
with receipt_data as (
    select a.receipt_id,
      a.receipt_usd_total_price as usd_total_price,
      a.payment_method,
      a.dc_payment_method,
      a.card_type,
      sr.receipt_group_id,
      date(a.creation_tsz) as date
    from `etsy-data-warehouse-prod.transaction_mart.all_receipts` a 
    join `etsy-data-warehouse-prod.etsy_shard.shop_receipts2` sr 
      on a.receipt_id = sr.receipt_id
    where date(a.creation_tsz) >= current_date - 90
    ),
  payment_data as (
    select payment_id,
      receipt_group_id,
      buyer_currency,
      payment_method 
    from `etsy-data-warehouse-prod.etsy_payments.payments` 
    where create_date >=  unix_seconds(timestamp(current_date - 90))
    ),
  cc_data as (
    select reference_id as payment_id,
      payment_gateway,
      card_type,
      currency,
      instrument_type
    from `etsy-data-warehouse-prod.etsy_payments.cc_txns`
    where create_date >=  unix_seconds(timestamp(current_date - 90))
      and txn_type = "PAYMENT"
      and status = "SETTLED"
      and instrument_type in ("CREDITCARD","K_INSTLMNT")
    ),
  all_data as (
    select r.receipt_id,
      r.date,
      r.usd_total_price,
      r.payment_method as receipt_payment_method,
      coalesce(p.payment_method, r.dc_payment_method) as dc_payment_method,
      cc.payment_gateway,
      case when coalesce(p.payment_method, r.dc_payment_method) = "dc_paypal" then "paypal"
        when cc.instrument_type = "K_INSTLMNT" then "klarna" 
        when cc.payment_gateway = 'access_worldpay' then 'worldpay'
        else cc.payment_gateway 
        end as payment_gateway_granular,
      coalesce(cc.currency, p.buyer_currency) as buyer_currency,
      coalesce(cc.card_type, r.card_type, "NA") as card_type
    from receipt_data r 
    left join payment_data p 
      on r.receipt_group_id = p.receipt_group_id
    left join cc_data cc 
      on p.payment_id = cc.payment_id
    ),
  -- AF UPDATES STARTING HERE
   granular_fee_holder as (
    select 
        case 
            when payment_method = "dc_paypal" then "paypal"
            when instrument_type = "K_INSTLMNT" then "klarna" 
            else payment_gateway 
        end as payment_gateway_granular, 
        currency,
        coalesce(card_type, "NA") as card_type,
        sum(etsy_payment_amount_usd) as etsy_payment_amount_usd,
        sum(case
        -- add gateway fee for adyen, its only available in proc_aggregated_fees_amount_usd with null card type so its not getting captured this way (using 2nd lowest gateway fee tier)
            when payment_gateway ='adyen' then (total_count*0.01) + proc_trx_fee_amount_usd + coalesce(proc_aggregated_fees_amount_usd,0)
            else proc_trx_fee_amount_usd + coalesce(proc_aggregated_fees_amount_usd,0)
        end) as total_fees
    --   round(sum(proc_trx_fee_amount_usd + coalesce(proc_aggregated_fees_amount_usd,0)) / sum(etsy_payment_amount_usd), 6) as processing_fee_rate -- we may want to add a least() function here to winsorize any crazy-high outlier permutations
    from etsy-data-warehouse-prod.rollups.ep_processor_fees
    where is_missing_cost = 0
    --   and etsy_trx_date >= current_date - 365
        and etsy_trx_date >= date_sub(date_trunc(current_date,month), INTERVAL 7 month) 
        and etsy_trx_date < date_sub(date_trunc(current_date,month), INTERVAL 1 month)
        and txn_type in ('PAYMENT','ADJUSTMENT')
    group by 1,2,3
),
granular_fee_rates as (
    select
        *
        ,case
        -- filter out wonky wp rates (due to some data being unavailable at certain periods)
            when payment_gateway_granular ='worldpay' and round(safe_divide(total_fees,etsy_payment_amount_usd),2) < 0.009 then null 
            else safe_divide(total_fees,etsy_payment_amount_usd)
        end as processing_fee_rate
    from granular_fee_holder
),
global_fee_rate as (
    select 
        currency,
        round(sum(proc_trx_fee_amount_usd+ coalesce(proc_aggregated_fees_amount_usd,0)) / sum(etsy_payment_amount_usd), 6) as global_fee_rate 
    from etsy-data-warehouse-prod.rollups.ep_processor_fees
    where is_missing_cost = 0
    and etsy_trx_date >= date_sub(date_trunc(current_date,month), INTERVAL 7 month) 
    and etsy_trx_date < date_sub(date_trunc(current_date,month), INTERVAL 1 month)
    and txn_type in ('PAYMENT','ADJUSTMENT')
    group by 1
)
  select r.*,
    f.processing_fee_rate,
    gfr.global_fee_rate,
    case when r.receipt_payment_method = "cc" then coalesce(f.processing_fee_rate, gfr.global_fee_rate) else 0 end as ep_fee_rate,
    case when r.receipt_payment_method = "cc" then coalesce(f.processing_fee_rate, gfr.global_fee_rate)*r.usd_total_price else 0 end as ep_processing_fee
  from all_data r 
  left join granular_fee_rates f 
    on r.payment_gateway_granular = f.payment_gateway_granular and r.buyer_currency = f.currency and r.card_type = f.card_type
  left join global_fee_rate gfr 
    on r.buyer_currency =gfr.currency;

drop table if exists `etsy-data-warehouse-dev.tnormil.receipt_level_take_rate` ;


create table `etsy-data-warehouse-dev.tnormil.receipt_level_take_rate` 
(receipt_id int64 OPTIONS(description="receipt_id from transactions mart"),
receipt_timestamp TIMESTAMP OPTIONS(description="timestamp of receipt; limited receipts from the past 90 days"),
transaction_rev float64 OPTIONS(description="actualized commission revenue"),
gross_ep_rev float64 OPTIONS(description="actualized gross ep payments revenue; this does not account for ep payments processing fees"),
ep_rev float64 OPTIONS(description="estimated net ep payments revenue; ep_rev = gross_ep_rev - total_ep_fees"),
total_ep_fees float64 OPTIONS(description="estimated total ep payments processing fees; total_ep_fees = ep_processing_fees + ep_cor"),
ep_cor float64 OPTIONS(description="estimated bad debt & CoR; Bad debt/cor amounts to 15% of the etsy payments gross_ep_rev"),
ep_processing_fee float64 OPTIONS(description="estimated ep payments fees based on historical processing fee rate by payment vendor applied to etsy payments gros ep rev"),
shipping_rev float64 OPTIONS(description="estimated based on historical average revenue earned per trade route"),
listing_rev float64 OPTIONS(description="estimated based a combination of revenue from the following: 1. we earn .20 for all orders with a quantity above 1, and 2. we earn .24 for each distinct listing id sold on an order"),
net_gms float64 OPTIONS(description="receipt gms"),
total_rev float64 OPTIONS(description="total_rev = transaction_rev + ep_rev + shipping_rev + listing_rev"))
 as  
  with receipts as (
    select rg.receipt_id,
      timestamp(rg.creation_tsz) as receipt_timestamp,
      a.transaction_revenue + a.transaction_shipping_revenue as transaction_rev,

      (a.ep_revenue + a.fx_hedge_revenue) as gross_ep_rev, 
      (a.ep_revenue + a.fx_hedge_revenue) - (coalesce(c.ep_processing_fee, 0) + (a.ep_revenue + a.fx_hedge_revenue)*0.15) as ep_rev,
      (coalesce(c.ep_processing_fee, 0) + (a.ep_revenue + a.fx_hedge_revenue)*0.15) as total_ep_fees, 
      coalesce(c.ep_processing_fee, 0) as ep_processing_fees, 
      (a.ep_revenue + a.fx_hedge_revenue)*0.15 as ep_cor,

      coalesce(b.avg_shipping_revenue_trailing_year, 0) as shipping_rev, 
      d.listing_fees as listing_rev,
      rg.gms_net
    from `etsy-data-warehouse-prod.transaction_mart.receipts_gms` rg
     left join commission_ep_rev a
      on rg.receipt_id = a.receipt_id
     left join shipping_rev_by_trade_route b
       on a.seller_country_grouped = b.seller_country_grouped and a.buyer_country_grouped = b.buyer_country_grouped
       and date(timestamp(rg.creation_tsz)) = b.order_date
     left join receipt_ep_fees c
      on rg.receipt_id = c.receipt_id  
    left join listing_fees d
       on rg.receipt_id = d.receipt_id  
    )
  select *,
     transaction_rev + ep_rev + shipping_rev + listing_rev as total_rev
  from receipts 
  ;

end
