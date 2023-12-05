---- Queries related to gift card redemptions

create or replace table etsy-data-warehouse-dev.tnormil.gc_receipts_redemption as 
  (select receipt_id, date(creation_tsz) as purchase_date, aat.mapped_user_id, buyer_type, count(*) as cnt
  from etsy-data-warehouse-prod.transaction_mart.all_transactions aat
  join etsy-data-warehouse-prod.rollups.giftcard_by_receipt using (receipt_id)
  left join receipt_data using (receipt_id)
  where date(creation_tsz) >= '2020-01-01'
  group by 1,2,3,4);

--- Buyer type distribution of gift card redemptions

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

select date_trunc(r.purchase_date,month) as month, buyer_type, count(distinct r.receipt_id) as receipts
from receipt_data r
join etsy-data-warehouse-dev.tnormil.gc_receipts_redemption g using (receipt_id)
group by 1,2
;

≈
-- % of Purchased Gift Cards are Claimed & Time between Purchase & Claim Date for those that are Redeemed in the US vs INTL markets, Filtered by Live Transactions

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
left join get_max using (purchase_quarter, region);

-- What share of purchased gift ards are claimed, redeemed, & exhausted? Filtered by live transactions 

SELECT date_trunc(date(timestamp_seconds(g.purchase_date)), month) as purchase_date, count(distinct g.giftcard_id) as gift_cards,
count(distinct case when g.claim_user_id is not null then g.giftcard_id end ) as claimed_gift_cards,
count(distinct case when g.first_use_date is not null then g.giftcard_id end ) as redeemed_gift_cards,
count(distinct case when g.first_use_date is not null and status = 'EXHAUSTED' then g.giftcard_id end ) as exhausted_gift_cards,
FROM `etsy-data-warehouse-prod.rollups.giftcard_usage` g
left join  `etsy-data-warehouse-prod.etsy_payments.giftcards` as a using (giftcard_id)
left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt on a.purchase_transaction_id = vt.transaction_id
WHERE
a.type = 'STANDARD'
and transaction_live = 1
and date(timestamp_seconds(a.purchase_date)) >= '2020-01-01'
group by 1
order by 1 desc;

-- How much value goes unsed on average by orignal gift card value?

SELECT date_trunc(date(timestamp_seconds(g.purchase_date)), month) as purchase_date, amount_start/100 as value,
avg(amount_current/100) as amount_current
FROM `etsy-data-warehouse-prod.rollups.giftcard_usage` g
left join  `etsy-data-warehouse-prod.etsy_payments.giftcards` as a using (giftcard_id)
left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt on a.purchase_transaction_id = vt.transaction_id
WHERE
a.type = 'STANDARD'
and transaction_live = 1
and date(timestamp_seconds(a.purchase_date)) >= '2020-01-01'
and g.first_use_date is not null
group by 1,2
order by 1 desc;

-- How many purchase days do redeemers usually use gift cards for?

with data_set as 
    (SELECT date_trunc(date(timestamp_seconds(g.purchase_date)), month) as purchase_date,
    g.giftcard_id,
    amount_start/100 as value,
    count(distinct gcr.receipt_id) as purchases,
    count(distinct date(payment_date)) as purchase_days
    FROM `etsy-data-warehouse-prod.rollups.giftcard_usage` g
    left join  `etsy-data-warehouse-prod.etsy_payments.giftcards` as a using (giftcard_id)
    left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` vt on a.purchase_transaction_id = vt.transaction_id
    left join etsy-data-warehouse-prod.rollups.giftcard_by_receipt gcr using (giftcard_id)
    WHERE
    a.type = 'STANDARD'
     and
    --claimed by a user
    a.claim_user_id is not null
    and transaction_live = 1
    and date(timestamp_seconds(a.purchase_date)) >= '2020-01-01'
    group by 1,2,3)
select purchase_date, value, avg(purchases) as purchases, avg(purchase_days) as purchase_days
from data_set
group by 1,2

--- RPR Stuff

-- repurchase rate by buyer type frequency 

with 
gc_purchases as 
(select date_trunc(gc.purchase_date,month) as month, gc.buyer_type, count(distinct gc.mapped_user_id) as buyers,
count(distinct r.mapped_user_id) as repurchase_30,
count(distinct r2.mapped_user_id) as repurchase_60,
from etsy-data-warehouse-dev.tnormil.gc_receipts_redemption gc
left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and r.purchase_date > gc.purchase_date and r.purchase_date < date_add(gc.purchase_date, interval 30 day)
left join receipt_data r2 on gc.mapped_user_id = r2.mapped_user_id and r2.purchase_date > gc.purchase_date and r2.purchase_date < date_add(gc.purchase_date, interval 60 day)
group by 1,2),
next_receipt as 
  (select *, lag(purchase_date) over (partition by mapped_user_id order by purchase_date desc) as next_purchase_date
  from receipt_data),
all_purchases as 
(select date_trunc(purchase_date, month) as month, buyer_type,  count(distinct mapped_user_id) as buyers,
count(distinct case when next_purchase_date < date_add(purchase_date, interval 60 day) then mapped_user_id end) as repurchase_60,
count(distinct case when next_purchase_date < date_add(purchase_date, interval 30 day) then mapped_user_id end) as repurchase_30,
from next_receipt
group by 1,2)
select g.month, g.buyer_type, g.buyers, g.repurchase_30, g.repurchase_60, a.buyers, a.repurchase_30, a.repurchase_60
from gc_purchases g
left join all_purchases a using (month,buyer_type)
