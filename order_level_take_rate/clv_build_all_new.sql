-- owner: vbhuta@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: this script allocates a user's LTV to the receipt_id level. this script builds the output table daily

BEGIN


CREATE TEMPORARY TABLE clv_current
  AS  WITH base AS (
    SELECT
        max(a.pred_date) AS max_date
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.analytics_clv` AS a
  )
  SELECT
      a.pred_date AS purchase_date,
      a.mapped_user_id,
      a.expectedgms8,
      a.expectedgms52,
      a.expectedgms104
    FROM
      `etsy-data-warehouse-prod.buyatt_mart.analytics_clv` AS a
      INNER JOIN base AS b ON a.pred_date = b.max_date
;

CREATE TEMPORARY TABLE clv_post
  AS  WITH base AS (
    SELECT
        l.mapped_user_id,
        l.purchase_date
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.ltv_user_data` AS l
      GROUP BY 1, 2
  )
  SELECT
      b.pred_date AS purchase_date,
      a.mapped_user_id,
      b.expectedgms8,
      b.expectedgms52,
      b.expectedgms104
    FROM
      base AS a
      INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.analytics_clv` AS b 
        ON a.mapped_user_id = b.mapped_user_id AND a.purchase_date = b.pred_date
;

CREATE TEMPORARY TABLE clv_pre
  AS  WITH base AS (
    SELECT
        l.mapped_user_id,
        date_sub(l.purchase_date, interval 1 DAY) AS pred_date
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.ltv_user_data` AS l
      GROUP BY 1, 2
  )
  SELECT
      date_add(a.pred_date, interval 1 DAY) AS purchase_date, -- converting back to purchase date to make user_purchases join easier
      a.mapped_user_id,
      b.expectedgms8,
      b.expectedgms52,
      b.expectedgms104
    FROM
      base AS a
      INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.analytics_clv` AS b 
        USING (mapped_user_id, pred_date) -- joining on the day before the purchase to get appropriate prediction
;

CREATE TEMPORARY TABLE clv_user_purchases
  AS SELECT
      a.mapped_user_id,
      a.purchase_date,
      a.expectedgms8 AS expectedgms8_post,
      coalesce(b.expectedgms8, 0) AS expectedgms8_pre,
      CASE
        WHEN coalesce(cast(a.expectedgms8 / nullif(b.expectedgms8, NUMERIC '0') as NUMERIC) - 1, 1) > 1 THEN CAST(1 as NUMERIC)
        WHEN coalesce(cast(a.expectedgms8 / nullif(b.expectedgms8, NUMERIC '0') as NUMERIC) - 1, 1) < 0 THEN CAST(0 as NUMERIC)
        ELSE coalesce(cast(a.expectedgms8 / nullif(b.expectedgms8, NUMERIC '0') as NUMERIC) - 1, 1)
      END AS ltv_jump,
      c.expectedgms52 AS expectedgms52_current,
      c.expectedgms104 AS expectedgms104_current,
      a.expectedgms52 AS expectedgms52_post,
      a.expectedgms104 AS expectedgms104_post
    FROM
      clv_post AS a
      LEFT OUTER JOIN clv_pre AS b USING (mapped_user_id, purchase_date)
      LEFT OUTER JOIN clv_current AS c ON a.mapped_user_id = c.mapped_user_id
;

--  calculate "blended" 2-mo GMS for each order (blend excluding that order), ie. actualizing GMS + adding remaining prediction
CREATE TEMPORARY TABLE time_calculations
  AS SELECT
      a.mapped_user_id,
      a.purchase_date,
      a.purchase_days,
      a.days_since AS days_since_last_purch,
      b.expectedgms8_post,
      b.ltv_jump,
      b.expectedgms52_current,
      b.expectedgms104_current,
      b.expectedgms52_post,
      b.expectedgms104_post,
      CASE
        WHEN date_diff(
          date_add(a.purchase_date, interval 60 DAY),
          date_sub(current_date(), interval 2 DAY),
          DAY) <= 0 THEN CAST(0 as NUMERIC)
        ELSE CAST(date_diff(
          date_add(a.purchase_date, interval 60 DAY),
          date_sub(current_date(), interval 2 DAY), 
          DAY) / 60 as NUMERIC)
      END AS time_remaining,
      sum(a.receipt_gms) AS day_gms
    FROM
      `etsy-data-warehouse-prod.buyatt_mart.ltv_user_data` AS a
      INNER JOIN clv_user_purchases AS b USING (mapped_user_id, purchase_date)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
;


CREATE TEMPORARY TABLE blend_calculations
  AS SELECT
      mapped_user_id,
      purchase_date,
      purchase_days,
      day_gms,
      days_since_last_purch,
      expectedgms8_post,
      ltv_jump,
      expectedgms52_current,
      expectedgms104_current,
      expectedgms52_post,
      expectedgms104_post,
      time_remaining,
      coalesce(sum(day_gms) OVER (
        PARTITION BY mapped_user_id ORDER BY unix_date(purchase_date) RANGE BETWEEN 1 FOLLOWING AND 60 FOLLOWING), CAST(0 as NUMERIC)) AS actualized_2mo_gms,
      coalesce(sum(day_gms) OVER (
        PARTITION BY mapped_user_id ORDER BY unix_date(purchase_date) RANGE BETWEEN 1 FOLLOWING AND 60 FOLLOWING), CAST(0 as NUMERIC)) 
        + (expectedgms8_post * time_remaining) AS blendgms8
    FROM
      time_calculations 
;

create or replace temp table dynamic_take_rate  as 
with summarized as 
(select mapped_user_id, date(receipt_timestamp) as purchase_date, sum(total_rev) as total_rev, sum(net_gms) as total_gms,
safe_divide( sum(total_rev) , sum(net_gms) ) as take_rate 
from `etsy-data-warehouse-dev.buyatt_mart.receipt_level_take_rate` r
join etsy-data-warehouse-prod.transaction_mart.receipts_gms rg on r.receipt_id = rg.receipt_id
join etsy-data-warehouse-prod.user_mart.user_profile up on rg.buyer_user_id = up.user_id
group by 1,2),
outlier as 
(  select distinct purchase_date, percentile_cont( take_rate, 0.98) over (partition by purchase_date) as take_rate_98_percentile
  from summarized )
select r.*, case when take_rate  >= take_rate_98_percentile then take_rate_98_percentile else take_rate end as win_take_rate
from summarized r
left join outlier o on r.purchase_date = o.purchase_date;


--  calculate LTV GMS for each order
CREATE TEMPORARY TABLE ltv_calc_daily
  AS  WITH gms_calc AS (
    SELECT
        mapped_user_id,
        purchase_date,
        day_gms,
        purchase_days,
        days_since_last_purch,
        CASE 
          when purchase_date<'2021-08-17' then -- making LTV pixel update to give more credit to new buyer and less to habitual
            CASE 
              WHEN purchase_days = 1 THEN coalesce(expectedgms104_current * 1.75 * ltv_jump * 0.6, 0)
              WHEN days_since_last_purch > 365 THEN coalesce(expectedgms52_current * ltv_jump * 0.6,0)
              WHEN purchase_days IN(
                2, 3
              ) THEN coalesce(expectedgms52_current * 1.75 * ltv_jump * 0.6,0)
              ELSE coalesce(blendgms8 * 0.9 * ltv_jump * 0.6,0)
            END
          else 
            CASE 
              WHEN purchase_days = 1 THEN coalesce(expectedgms104_current * 1.75 * 1.33 * ltv_jump * 0.6, 0)
              WHEN days_since_last_purch > 365 THEN coalesce(expectedgms52_current * ltv_jump * 0.6, 0)
              WHEN purchase_days IN(
                2, 3
              ) THEN coalesce(expectedgms52_current * 1.75 * ltv_jump * 0.6, 0)
              WHEN purchase_days <10 THEN coalesce(blendgms8 * 0.9 * ltv_jump * 0.6, 0)
              ELSE 0
            END
        END AS ltv_only_gms_moving,
        CASE 
          when purchase_date<'2021-08-17' then -- making LTV pixel update to give more credit to new buyer and less to habitual
            CASE
              WHEN purchase_days = 1 THEN coalesce(expectedgms104_post * 1.75 * ltv_jump * 0.6, 0)
              WHEN days_since_last_purch > 365 THEN coalesce(expectedgms52_post * ltv_jump * 0.6, 0)
              WHEN purchase_days IN(
                2, 3
              ) THEN coalesce(expectedgms52_post * 1.75 * ltv_jump * 0.6, 0)
              ELSE coalesce(expectedgms8_post * 0.9 * ltv_jump * 0.6, 0)
            END
          else 
            CASE
              WHEN purchase_days = 1 THEN coalesce(expectedgms104_post * 1.75 * 1.33 * ltv_jump * 0.6, 0)
              WHEN days_since_last_purch > 365 THEN coalesce(expectedgms52_post * ltv_jump * 0.6, 0)
              WHEN purchase_days IN(
                2, 3
              ) THEN coalesce(expectedgms52_post * 1.75 * ltv_jump * 0.6, 0)
              WHEN purchase_days <10 THEN coalesce(expectedgms8_post * 0.9 * ltv_jump * 0.6, 0)
              ELSE 0
            END
        END AS ltv_only_gms_static
      FROM
      blend_calculations
  )
  SELECT
      mapped_user_id,
      purchase_date,
      day_gms,
      day_gms + ltv_only_gms_moving as ltv_gms_moving,
      day_gms + ltv_only_gms_static as ltv_gms_static,
      
      CASE
        WHEN purchase_date < DATE '2015-01-01' THEN (day_gms + ltv_only_gms_moving) * 0.054
        WHEN purchase_date >= DATE '2015-01-01'
         AND purchase_date < DATE '2017-01-01' THEN (day_gms + ltv_only_gms_moving) * 0.062
        WHEN purchase_date >= DATE '2017-01-01'
         AND purchase_date < DATE '2018-01-01' THEN (day_gms + ltv_only_gms_moving) * 0.079
        WHEN purchase_date >= DATE '2018-01-01'
         AND purchase_date < DATE '2018-07-16' THEN (day_gms + ltv_only_gms_moving) * 0.081
        WHEN purchase_date >= DATE '2018-07-16'
         AND purchase_date < DATE '2022-04-11' THEN (day_gms + ltv_only_gms_moving) * 0.102
        WHEN purchase_date >= DATE '2022-04-11'
         AND purchase_date < DATE '2022-05-09' THEN (day_gms + ltv_only_gms_moving) * 0.112
        WHEN purchase_date >= DATE '2022-05-09'
         AND purchase_date < DATE '2024-02-01' THEN (day_gms + ltv_only_gms_moving) * 0.115
        WHEN purchase_date >= DATE '2024-02-01' THEN (ltv_only_gms_moving * 0.165) + (day_gms * win_take_rate)
        ELSE (ltv_only_gms_moving * 0.165) + (day_gms * win_take_rate) 
      END AS attr_rev_moving,

      CASE
        WHEN purchase_date < DATE '2015-01-01' THEN (ltv_only_gms_static + day_gms) * 0.054
        WHEN purchase_date >= DATE '2015-01-01'
         AND purchase_date < DATE '2017-01-01' THEN (ltv_only_gms_static + day_gms) * 0.062
        WHEN purchase_date >= DATE '2017-01-01'
         AND purchase_date < DATE '2018-01-01' THEN (ltv_only_gms_static + day_gms) * 0.079
        WHEN purchase_date >= DATE '2018-01-01'
         AND purchase_date < DATE '2018-07-16' THEN (ltv_only_gms_static + day_gms) * 0.081
        WHEN purchase_date >= DATE '2018-07-16'
         AND purchase_date < DATE '2022-04-11' THEN (ltv_only_gms_static + day_gms) * 0.102
        WHEN purchase_date >= DATE '2022-04-11'
         AND purchase_date < DATE '2022-05-09' THEN (ltv_only_gms_static + day_gms) * 0.112
        WHEN purchase_date >= DATE '2022-05-09'
         AND purchase_date < DATE '2022-12-09' THEN (ltv_only_gms_static + day_gms) * 0.115
        WHEN purchase_date >= DATE '2022-12-09'
         AND purchase_date < DATE '2024-02-01' THEN (ltv_only_gms_static + day_gms) * 0.118
        WHEN purchase_date >= DATE '2024-02-01' THEN (ltv_only_gms_static * 0.168) + (day_gms * win_take_rate) 
        ELSE (ltv_only_gms_static * 0.168) + (day_gms * win_take_rate) 
      END AS attr_rev_static,
  
      CASE
        WHEN purchase_date < DATE '2015-01-01' THEN ltv_only_gms_static * 0.054
        WHEN purchase_date >= DATE '2015-01-01'
         AND purchase_date < DATE '2017-01-01' THEN ltv_only_gms_static * 0.062
        WHEN purchase_date >= DATE '2017-01-01'
         AND purchase_date < DATE '2018-01-01' THEN ltv_only_gms_static * 0.079
        WHEN purchase_date >= DATE '2018-01-01'
         AND purchase_date < DATE '2018-07-16' THEN ltv_only_gms_static * 0.081
        WHEN purchase_date >= DATE '2018-07-16'
         AND purchase_date < DATE '2022-04-11' THEN ltv_only_gms_static * 0.102
        WHEN purchase_date >= DATE '2022-04-11'
         AND purchase_date < DATE '2022-05-09' THEN ltv_only_gms_static * 0.112
        WHEN purchase_date >= DATE '2022-05-09'
         AND purchase_date < DATE '2022-12-09' THEN ltv_only_gms_static * 0.115
        WHEN purchase_date >= DATE '2022-12-09'
         AND purchase_date < DATE '2024-02-01' THEN ltv_only_gms_static * 0.118
        -- this is where we need to add the addl prolist rev to ltv_only_gms_static specifically
        WHEN purchase_date >= DATE '2024-02-01' THEN (ltv_only_gms_static * 0.168) 
        ELSE (ltv_only_gms_static * 0.168) 
      END AS ltv_rev
    FROM
      gms_calc
    left join dynamic_take_rate using (mapped_user_id, purchase_date)
;

--  calculate etsy ads (OSA) revenue for each order
CREATE TEMPORARY TABLE etsy_ads_data
  AS SELECT
      receipt_id,
      status,
      CAST(acquisition_fee_usd / 100 AS NUMERIC) AS etsy_ads_revenue
    FROM
      `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts`
;

--  This is the output table
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.buyatt_mart.buyatt_analytics_clv`
  AS  WITH base AS (
    SELECT
        a.mapped_user_id,
        a.purchase_date,
        a.receipt_id,
        ROUND(a.receipt_gms, 2) AS receipt_gms,
        a.purchase_days AS purchase_day_number,
        a.receipt_percentage AS day_percent,
        a.days_since AS days_since_last_purch,
        ROUND(b.day_gms, 2) AS day_gms,
        ROUND(b.ltv_gms_moving * a.receipt_percentage, 2) AS ltv_gms_moving,
        ROUND(b.attr_rev_moving * a.receipt_percentage, 2) AS attr_rev_moving,
        ROUND(b.ltv_gms_static * a.receipt_percentage, 2) AS ltv_gms,
        ROUND(b.attr_rev_static * a.receipt_percentage, 2) AS attr_rev,
        ROUND(b.ltv_rev * a.receipt_percentage, 2) AS ltv_rev,
        CASE
          WHEN a.purchase_days = 1 THEN 'new_buyer'
          WHEN a.days_since <= 7 THEN 'days_since_last_purchase_less_7'
          WHEN a.days_since <= 60 THEN 'days_since_last_purchase_7_to_60'
          WHEN a.days_since <= 365 THEN 'days_since_last_purchase_60_to_365'
          WHEN a.days_since <= 545 THEN 'days_since_last_purchase_365_to_545'
          ELSE 'days_since_last_purchase_545_plus'
        END AS recency,
        CASE
          WHEN a.purchase_days = 1 THEN 'new_buyer'
          WHEN a.days_since > 365 THEN 'reactivated_buyer'
          ELSE 'existing_buyer'
        END AS buyer_type
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.ltv_user_data` AS a
        LEFT OUTER JOIN ltv_calc_daily AS b 
        USING (mapped_user_id, purchase_date)
  )
  SELECT
      a.mapped_user_id,
      a.purchase_date,
      a.receipt_id,
      a.receipt_gms,
      a.purchase_day_number,
      a.day_percent,
      a.days_since_last_purch,
      a.day_gms,
      a.ltv_gms_moving, 
      CASE
        WHEN b.status = 1 THEN a.attr_rev_moving + coalesce(b.etsy_ads_revenue, 0)
        ELSE a.attr_rev_moving
      END AS attr_rev_moving, -- don't include etsy ads (OSA) revenue until after chargeablity begins
      a.ltv_gms,
      CASE
        WHEN b.status = 1 THEN a.attr_rev + coalesce(b.etsy_ads_revenue, 0)
        ELSE a.attr_rev
      END AS attr_rev, -- don't include etsy ads  (OSA) revenue until after chargeablity begins
      a.ltv_rev,
      CASE
        WHEN b.status = 1 THEN b.etsy_ads_revenue
        ELSE CAST(0 as NUMERIC)
      END AS etsy_ads_revenue,
      CASE
        WHEN b.status <> 1 THEN b.etsy_ads_revenue
        ELSE CAST(0 as NUMERIC)
      END AS etsy_ads_revenue_not_charged, --  can remove this once we start charging
      a.recency,
      a.buyer_type
    FROM
      base AS a
      LEFT OUTER JOIN etsy_ads_data AS b 
      USING (receipt_id)
;
END
-- 59 s
--  Call to analyze_statistics() ignored.

--  applying scale up band-aid to new, 2 and 3x buyers to account for underestimated LTV
-- start of grace
-- start of grace
--  don't include etsy ads revenue until after chargeablity begins
--  don't include etsy ads revenue until after chargeablity begins
--  select count(*) from clv_post;
--  select count(*) from clv_pre ;
--  select count(*) from clv_current;
--  select count(*) from clv_user_purchases;
--  select count(*) from time_calculations;
--  select count(*) from blend_calculations;
--  select count(*) from ltv_calc_daily;
