---- Queries related to gift card purchases

--- Share of receipts related to gift cards

create table if not exists etsy-data-warehouse-dev.tnormil.gc_receipts as 
  (select receipt_id, price, date(creation_tsz) as purchase_date, mapped_user_id, count(*) as cnt
  from etsy-data-warehouse-prod.transaction_mart.all_transactions aat
  where date(creation_tsz) >= '2020-01-01' and aat.is_gift_card = 1
  group by 1,2,3,4);

-- % of receipts that are gift card related by marketing region and device 

select date(timestamp_trunc(rg.creation_tsz, week)) as date_week, 
date(timestamp_trunc(rg.creation_tsz, quarter)) as date_qtr, 
date(timestamp_trunc(rg.creation_tsz, year)) as date_yr, 
case when extract(month from timestamp_trunc(rg.creation_tsz, quarter)) = 10 then 'Holiday' else 'Rest of year' end as timeframe,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
case when lower(v.mapped_platform_type) like '%mweb%android%' then "Android Mobile Web"
when lower(v.mapped_platform_type) like '%boe%android%'  then "Android BOE"
when lower(v.mapped_platform_type) like '%mweb%ios%' then "iOS Mobile Web"
when lower(v.mapped_platform_type) like '%boe%ios%' then "iOS BOE"
else "Desktop" end as device,
count(case when gc.receipt_id is not null then rg.receipt_id end) as is_gift_card_receipts,
count(distinct rg.receipt_id) as receipts
from etsy-data-warehouse-prod.transaction_mart.receipts_gms rg
left join etsy-data-warehouse-dev.tnormil.gc_receipts gc using (receipt_id)
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
where date(rg.creation_tsz) >= '2020-01-01'
group by 1,2,3,4,5,6;

--- Buyer type distribution stuff

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
    WHERE extract(YEAR from CAST(purchase_date as DATETIME)) >= 2018
;
 
-- buyer type distibution by frequency 

with gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null)
select date_trunc(purchase_date,month) as month, buyer_type_new, count(distinct receipt_id) as receipts
from gms_share_buyer_type
group by 1,2
;

-- repurchase rate by buyer type frequency 

with receipt_date as 
    (select purchase_date, mapped_user_id, buyer_type, count(*)
    from receipt_data
    group by 1,2,3),
gms_share_buyer_type as 
    (select gc.*, r.buyer_type, 
      CASE when date_diff(gc.purchase_date, r.purchase_date, day) = 0 then r.buyer_type else 
      CASE 
        WHEN buyer_type is null THEN 'new_buyer'
        WHEN date_diff(gc.purchase_date, r.purchase_date, day) >= 365 then 'reactivated_buyer'
        WHEN purchase_day_number + 1 = 2
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '2x_buyer'
        WHEN purchase_day_number + 1 = 3
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '3x_buyer'
        WHEN purchase_day_number + 1 >= 4 and purchase_day_number + 1 <= 9
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '4_to_9x_buyer'
        WHEN purchase_day_number + 1 >= 10
        AND date_diff(gc.purchase_date, r.purchase_date, day) < 365 THEN '10plus_buyer'
        ELSE 'other'
      END END AS buyer_type_new,
    date_diff(gc.purchase_date, r.purchase_date, day) as days_between,
    row_number() over (partition by gc.receipt_id order by date_diff(gc.purchase_date, r.purchase_date, day) asc) as rnk
    from etsy-data-warehouse-dev.tnormil.gc_receipts gc
    left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and gc.purchase_date >= r.purchase_date
    qualify rnk = 1 or rnk is null),
gc_purchases as 
(select date_trunc(gc.purchase_date,month) as month, buyer_type_new as buyer_type, count(distinct gc.mapped_user_id) as buyers,
count(distinct r.mapped_user_id) as repurchase_30,
count(distinct r2.mapped_user_id) as repurchase_60,
from gms_share_buyer_type gc
left join receipt_data r on gc.mapped_user_id = r.mapped_user_id and r.purchase_date > gc.purchase_date and r.purchase_date < date_add(gc.purchase_date, interval 30 day)
left join receipt_data r2 on gc.mapped_user_id = r2.mapped_user_id and r2.purchase_date > gc.purchase_date and r2.purchase_date < date_add(gc.purchase_date, interval 60 day)
group by 1,2),
next_receipt as 
  (select *, lag(purchase_date) over (partition by mapped_user_id order by purchase_date desc) as next_purchase_date
  from receipt_date),
all_purchases as 
(select date_trunc(purchase_date, month) as month, buyer_type,  count(distinct mapped_user_id) as buyers,
count(distinct case when next_purchase_date < date_add(purchase_date, interval 60 day) then mapped_user_id end) as repurchase_60,
count(distinct case when next_purchase_date < date_add(purchase_date, interval 30 day) then mapped_user_id end) as repurchase_30,
from next_receipt
group by 1,2)
select g.month, g.buyer_type, g.buyers, g.repurchase_30, a.buyers, a.repurchase_30
from gc_purchases g
left join all_purchases a using (month,buyer_type)

--- AOV stuff

-- Past Year Distribution of Gift Cards Purchased by Original Gift Card Value & Key Market

SELECT amount_start/100 as value,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
count(distinct aat.transaction_id) as transactions
FROM
`etsy-data-warehouse-prod.etsy_payments.giftcards` as a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` aat on a.purchase_transaction_id = aat.transaction_id
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
WHERE a.type = 'STANDARD'
and date(aat.creation_tsz) >= date_sub(current_date - 1, interval 1 year)
and transaction_live = 1
group by 1, 2;

-- Avg transaction value of GC purchase by region over time

SELECT date_trunc(date(aat.creation_tsz), month) as date_month,
date_trunc(date(aat.creation_tsz), year) as date_year,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
sum(amount_start/100) as sum_value,
count(distinct aat.transaction_id) as transactions,
sum(aat.quantity) as quantity
FROM
`etsy-data-warehouse-prod.etsy_payments.giftcards` as a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` aat on a.purchase_transaction_id = aat.transaction_id
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
WHERE a.type = 'STANDARD'
and date(aat.creation_tsz) >= '2021-01-01'
and transaction_live = 1
group by 1, 2, 3;

-- Median transaction value of GC purchase by region over time

SELECT distinct date_trunc(date(aat.creation_tsz), month) as date_month,
date_trunc(date(aat.creation_tsz), year) as date_year,
case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end as key_market, 
PERCENTILE_DISC(aat.quantity, 0.5) OVER(partition by date_trunc(date(aat.creation_tsz), month), case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end) AS median_quantity,
PERCENTILE_DISC(amount_start/100, 0.5) OVER(partition by date_trunc(date(aat.creation_tsz), month), case when marketing_region in ('US','GB','DE','FR','CA') then marketing_region else 'Row' end) AS median_quantity
FROM
`etsy-data-warehouse-prod.etsy_payments.giftcards` as a
join `etsy-data-warehouse-prod.transaction_mart.all_transactions` aat on a.purchase_transaction_id = aat.transaction_id
left join etsy-data-warehouse-prod.visit_mart.visits_transactions vt using (receipt_id)
left join etsy-data-warehouse-prod.buyatt_mart.visits v using (visit_id)
WHERE a.type = 'STANDARD'
and date(aat.creation_tsz) >= '2021-01-01'
and transaction_live = 1;
