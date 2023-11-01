---- Queries related to gift card redemptions

--- Buyer type distribution of gift card redemptions

create table if not exists etsy-data-warehouse-dev.tnormil.gc_receipts_redemption as 
  (select receipt_id, date(creation_tsz) as purchase_date, mapped_user_id, count(*) as cnt
  from etsy-data-warehouse-prod.transaction_mart.all_transactions aat
  join etsy-data-warehouse-prod.rollups.giftcard_by_receipt using (receipt_id)
  where date(creation_tsz) >= '2020-01-01'
  group by 1,2,3);

CREATE OR REPLACE TEMPORARY TABLE receipt_data
  AS SELECT
      receipt_id,
      mapped_user_id,
      purchase_date,
      purchase_day_number,
      coalesce(days_since_last_purch, 0) AS days_since_last_purch,
      CASE
            WHEN buyer_type = 'new_buyer' THEN 'new_buyer'
            WHEN purchase_day_number = 2
            AND buyer_type <> 'reactivated_buyer' THEN '2x_buyer'
            WHEN purchase_day_number = 3
            AND buyer_type <> 'reactivated_buyer' THEN '3x_buyer'
            WHEN purchase_day_number >= 4 and purchase_day_number<= 9
            AND buyer_type <> 'reactivated_buyer' THEN '4_to_9x_buyer'
            WHEN purchase_day_number >= 10
            AND buyer_type <> 'reactivated_buyer' THEN '10plus_buyer'
            WHEN buyer_type = 'reactivated_buyer' THEN 'reactivated_buyer'
            ELSE 'other'
          END AS buyer_type,
      recency,
      day_percent,
      attr_rev AS ltv_revenue,
      receipt_gms + (ltv_gms - day_gms) * day_percent AS ltv_gms,
      receipt_gms AS gms
    FROM
      `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv`
    WHERE extract(YEAR from CAST(purchase_date as DATETIME)) >= 2018;

--- What does the redemption funnel look like?
-- % of Purchased Gift Cards are Redeemed & Time between Purchase & Redemption for those that are Redeemed in the US vs INTL markets, Filtered by Live Transactions

with base_data as
      (SELECT DATE_TRUNC(date(timestamp_seconds(a.purchase_date)), quarter) purchase_quarter,
      case when marketing_region = 'US' then 'US' else 'INTL' end as region,
      case when date_diff(DATE(timestamp_seconds(a.claim_date) ) , DATE(timestamp_seconds(a.purchase_date) ), day)  <= 10 then date_diff(DATE(timestamp_seconds(a.claim_date) ) , DATE(timestamp_seconds(a.purchase_date) ), day) 
      when date_diff(DATE(timestamp_seconds(a.claim_date) ) , DATE(timestamp_seconds(a.purchase_date) ), day) <= 100 then round(date_diff(DATE(timestamp_seconds(a.claim_date) ) , DATE(timestamp_seconds(a.purchase_date) ), day) , -1)
      when date_diff(DATE(timestamp_seconds(a.claim_date) ) , DATE(timestamp_seconds(a.purchase_date) ), day) <= 800 then  round(date_diff(DATE(timestamp_seconds(a.claim_date) ) , DATE(timestamp_seconds(a.purchase_date) ), day) , -2)
      else 800
      end as date_diff,
      count(distinct a.purchase_transaction_id) as gift_cards
      FROM
      `etsy-data-warehouse-prod.etsy_payments.giftcards` as a
      left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt on a.purchase_transaction_id = vt.transaction_id
      left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
      WHERE
      a.type = 'STANDARD'
      and
      --claimed by a user
      a.claim_user_id is not null
      and transaction_live = 1
      and date(timestamp_seconds(a.purchase_date)) >= '2020-01-01'
      group by 1,2,3
      order by 1 desc),
get_max as 
    (select DATE_TRUNC(date(timestamp_seconds(a.purchase_date)), quarter) purchase_quarter,
    case when marketing_region = 'US' then 'US' else 'INTL' end as region,
    count(distinct a.purchase_transaction_id) as tot
    FROM
      `etsy-data-warehouse-prod.etsy_payments.giftcards` as a
      left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt on a.purchase_transaction_id = vt.transaction_id
      left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
      WHERE a.type = 'STANDARD'
      and transaction_live = 1
    group by 1,2)
select distinct purchase_quarter, region, date_diff,
sum(gift_cards) over (partition by purchase_quarter,region order by date_diff asc) as running_tot_gift_cards, tot,
from base_data
left join get_max using (purchase_quarter, region)

select date_trunc(r.purchase_date,month) as month, buyer_type, count(distinct r.receipt_id) as receipts
from receipt_data r
join etsy-data-warehouse-dev.tnormil.gc_receipts_redemption g using (receipt_id)
group by 1,2
;
