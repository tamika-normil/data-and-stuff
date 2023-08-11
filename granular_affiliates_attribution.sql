-- owner: mkim@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: rollup that pulls attribution metrics for content publishers
-- whose visits tend to get under-attributed due to the nature of
-- their advertising platforms for affiliate marketing
 /*
step 1:
limit awin spend data to all clicks within the last 2 months that resulted in sale
get the receipt data for those clicks
 */
BEGIN

CREATE OR REPLACE TEMPORARY TABLE awin_monthly
  AS SELECT
      publisher_id,
      click_date AS click_timestamp,
      transaction_date AS order_timestamp,
      order_ref AS receipt_id
    FROM
      `etsy-data-warehouse-prod.marketing.awin_spend_data`
    WHERE DATE(datetime_trunc(Datetime(click_date), MONTH)) >= date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE temp_attr_by_browser
  AS SELECT
      a.buy_visit_id,
      a.o_visit_id,
      a.o_visit_run_date,
      a.receipt_id,
      m.publisher_id,
      m.click_timestamp,
      a.receipt_timestamp,
      a.buy_date,
      DATE(timestamp_seconds(a.buy_date)) AS day_key,
      a.receipt_market,
      split(a.buy_visit_id, '.')[SAFE_ORDINAL(1)] AS browser_id,
      b.start_datetime AS buy_visit_timestamp,
      a.buyer_type,
      a.gms,
      a.gms_gross,
      CASE
        WHEN b.third_channel in ('affiliates_feed', 'affiliates_widget') THEN 1
        ELSE 0
      END AS from_product_feeds,
      b.key_market
    FROM
      `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS a
      INNER JOIN awin_monthly AS m ON a.receipt_id = CAST(m.receipt_id as INT64)
       AND CAST(m.click_timestamp as TIMESTAMP) <= a.receipt_timestamp
      INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` AS b ON a.buy_visit_id = b.visit_id
;

 /*
step 2:
removes any sales that are probably not under attributed since the clicks have overlapping visit data within 5 minutes that already have affiliate touchpoints
 */
CREATE OR REPLACE TEMPORARY TABLE buyatt_awin_test_affiliatematch
  AS  WITH base AS (
    SELECT
        b.o_visit_id,
        b.receipt_id,
        CAST(timestamp_seconds(b.o_visit_run_date) as DATETIME) AS o_visit_run_date_fmt,
        -- b.order_timestamp as awin_order_timestamp,
        b.click_timestamp AS awin_click_timestamp,
        c.top_channel,
        c.second_channel,
        c.third_channel
      FROM
        temp_attr_by_browser AS b
        LEFT OUTER JOIN (select * from `etsy-data-warehouse-prod.weblog.visits` where _date>'2000-01-01') AS c ON b.o_visit_id = c.visit_id
      WHERE c.start_datetime >= b.click_timestamp
       AND Datetime(c.start_datetime) <= datetime_add(Datetime(b.click_timestamp), interval 5 MINUTE)
  )
  SELECT
      base.o_visit_id,
      base.receipt_id,
      base.o_visit_run_date_fmt,
      base.awin_click_timestamp,
      base.top_channel,
      base.second_channel,
      base.third_channel
    FROM
      base
    WHERE regexp_contains(base.top_channel, '(\?i)affiliates')
     OR regexp_contains(base.second_channel, '(\?i)affiliates')
;

CREATE OR REPLACE TEMPORARY TABLE awin_deduped
  AS SELECT
      buy_visit_id,
      o_visit_id,
      o_visit_run_date,
      receipt_id,
      publisher_id,
      click_timestamp,
      receipt_timestamp,
      buy_date,
      day_key,
      receipt_market,
      browser_id,
      buy_visit_timestamp,
      buyer_type,
      gms,
      gms_gross,
      from_product_feeds,
      key_market
    FROM
      temp_attr_by_browser
    WHERE receipt_id NOT IN(
      SELECT
          distinct buyatt_awin_test_affiliatematch.receipt_id
        FROM
          buyatt_awin_test_affiliatematch
    )
;

CREATE OR REPLACE TEMPORARY TABLE buyatt_awin_receipt_slice
  AS SELECT DISTINCT
      awin_deduped.receipt_id,
      awin_deduped.receipt_timestamp,
      awin_deduped.buy_date,
      awin_deduped.day_key,
      awin_deduped.receipt_market,
      awin_deduped.buy_visit_id,
      awin_deduped.browser_id,
      awin_deduped.buy_visit_timestamp,
      awin_deduped.buyer_type,
      awin_deduped.gms,
      awin_deduped.gms_gross
    FROM
      awin_deduped
;

 /*
step 3:
code from this point on take the existing attribution model and inserts affiliate click data
this step specifically finds visits eligible for attribution for only the receipts that may be underattributed
 */
CREATE OR REPLACE TEMPORARY TABLE buy_visit_mapping
  AS  WITH buys AS (
    SELECT
        buyatt_awin_receipt_slice.buy_visit_id,
        buyatt_awin_receipt_slice.buy_visit_timestamp,
        buyatt_awin_receipt_slice.day_key,
        buyatt_awin_receipt_slice.receipt_id,
        buyatt_awin_receipt_slice.browser_id
      FROM
        buyatt_awin_receipt_slice
  )
  SELECT
      a.buy_visit_id,
      a.buy_visit_timestamp,
      a.day_key,
      a.receipt_id,
      b.browser_id,
      coalesce(b.maps_to_browser, a.browser_id) AS maps_to_browser
    FROM
      buys AS a
      LEFT OUTER JOIN `etsy-data-warehouse-prod.browser_mart.browser_cross_mapping_buyatt` AS b USING (browser_id)
;

--  get max date of visit before window
CREATE OR REPLACE TEMPORARY TABLE buyatt_priorvisits
  AS  WITH priordates AS (
    SELECT
        epoch_s,
        date
      FROM
        `etsy-data-warehouse-prod.public.calendar_dates`
      WHERE epoch_s >= (
        SELECT
            UNIX_SECONDS(CAST(CAST(min(buyatt_awin_receipt_slice.day_key) as DATETIME) AS TIMESTAMP))
          FROM
            buyatt_awin_receipt_slice
      ) - 86400 * 90
       AND epoch_s <= (
        SELECT
            UNIX_SECONDS(CAST(CAST(min(buyatt_awin_receipt_slice_0.day_key) as DATETIME) AS TIMESTAMP))
          FROM
            buyatt_awin_receipt_slice AS buyatt_awin_receipt_slice_0
      ) - 86400 * 30
  ), prior_visits AS (
    SELECT DISTINCT
        v.visit_id,
        v.run_date,
        v.start_datetime,
        v.browser_id,
        v.partition_key
      FROM
        `etsy-data-warehouse-prod.visit_mart.visits` AS v
      WHERE v.partition_key >= (
        SELECT
            min(priordates.epoch_s)
          FROM
            priordates
      )
       AND v.run_date IN(
        SELECT
            priordates_0.epoch_s
          FROM
            priordates AS priordates_0
      )
       AND v.browser_id IN(
        SELECT
            buy_visit_mapping.maps_to_browser
          FROM
            buy_visit_mapping
      )
       AND v.platform_app <> 'soe'
  )
  SELECT
      b.buy_visit_id,
      max(p.start_datetime) AS max_visit_before_window
    FROM
      buy_visit_mapping AS b
      INNER JOIN prior_visits AS p ON b.maps_to_browser = p.browser_id
    WHERE Datetime(p.start_datetime) < datetime_sub(Datetime(b.buy_visit_timestamp), interval 30 DAY)
     AND Datetime(p.start_datetime) > datetime_sub(Datetime(b.buy_visit_timestamp), interval 90 DAY)
    GROUP BY b.browser_id, 1
;

--  get visit data for all visits within 30 days of purchase visit
--  9/30/19: att utm_content in order to get attribution specifically for contet publishers
CREATE OR REPLACE TEMPORARY TABLE buyatt_currvisits
  AS  WITH rundates AS (
    SELECT
        epoch_s
      FROM
        `etsy-data-warehouse-prod.public.calendar_dates`
      WHERE epoch_s >= (
        SELECT
            UNIX_SECONDS(CAST(CAST(min(buyatt_awin_receipt_slice.day_key) as DATETIME) AS TIMESTAMP))
          FROM
            buyatt_awin_receipt_slice
      ) - 86400 * 30
       AND epoch_s <= (
        SELECT
            UNIX_SECONDS(CAST(CAST(max(buyatt_awin_receipt_slice_0.day_key) as DATETIME) AS TIMESTAMP))
          FROM
            buyatt_awin_receipt_slice AS buyatt_awin_receipt_slice_0
      )
  ), window_visits AS (
    SELECT
        v.browser_id,
        v.run_date,
        v.second_channel,
        v.utm_content,
        v.start_datetime,
        v.visit_id,
        v.paid,
        v.has_referral,
        v.external_source,
        v.partition_key,
        CASE
          WHEN v.third_channel in ('affiliates_feed', 'affiliates_widget') THEN 1
          ELSE 0
        END AS from_product_feeds,
        v.key_market
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.visits` AS v
      WHERE v.partition_key >= (
        SELECT
            min(rundates.epoch_s)
          FROM
            rundates
      )
       AND v.run_date IN(
        SELECT
            rundates_0.epoch_s
          FROM
            rundates AS rundates_0
      )
       AND v.browser_id IN(
        SELECT
            buy_visit_mapping.maps_to_browser
          FROM
            buy_visit_mapping
      )
       AND v.platform_app <> 'soe'
  )
  SELECT
      b.browser_id,
      b.buy_visit_id,
      b.receipt_id,
      w.visit_id,
      w.start_datetime,
      w.second_channel,
      w.utm_content,
      w.run_date,
      w.paid,
      w.has_referral,
      w.external_source,
      w.partition_key,
      w.from_product_feeds,
      w.key_market
    FROM
      buy_visit_mapping AS b
      INNER JOIN window_visits AS w ON b.maps_to_browser = w.browser_id
    WHERE w.start_datetime <= b.buy_visit_timestamp
     AND Datetime(w.start_datetime) >= datetime_sub(Datetime(b.buy_visit_timestamp), interval 30 DAY)
     AND w.run_date <= UNIX_SECONDS(CAST(CAST(b.day_key as DATETIME) AS TIMESTAMP))
;

 /*
step 4:
merge buyatt_currvisits with the missing visits from the external awin data source
removes any awin clicks overlapping visits that are eligible for attribution that occurred within 10 minutes of the click
 */
CREATE OR REPLACE TEMPORARY TABLE affiliate_join
  AS SELECT DISTINCT
      NULL AS browser_id,
      a.buy_visit_id,
      a.receipt_id,
      CAST(UNIX_SECONDS(CAST(a.click_timestamp AS TIMESTAMP)) as STRING) AS visit_id,
      a.click_timestamp AS start_datetime,
      'affiliates' AS second_channel,
      substr(CAST(a.publisher_id as STRING), 1, 80) AS utm_content,
      UNIX_SECONDS(CAST(CAST(DATE(a.click_timestamp) as DATETIME) AS TIMESTAMP)) AS run_date,
      1 AS paid,
      1 AS has_referral,
      1 AS external_source,
      UNIX_SECONDS(CAST(CAST(DATE(a.click_timestamp) as DATETIME) AS TIMESTAMP)) AS partition_key,
      a.from_product_feeds,
      a.key_market
    FROM
      awin_deduped AS a
;

CREATE OR REPLACE TEMPORARY TABLE affiliate_currvisits
  AS  WITH dedupe as ( 
        SELECT DISTINCT b.visit_id, b.receipt_id, a.visit_id as orig_visit_id
        FROM buyatt_currvisits  AS a
        INNER JOIN affiliate_join AS b USING (receipt_id)
        WHERE a.second_channel = 'affiliates'
        AND UNIX_SECONDS(CAST(a.start_datetime AS TIMESTAMP)) - UNIX_SECONDS(CAST(b.start_datetime AS TIMESTAMP)) <= 600
        AND UNIX_SECONDS(CAST(a.start_datetime AS TIMESTAMP)) - UNIX_SECONDS(CAST(b.start_datetime AS TIMESTAMP)) >= -600), 
  affiliate_currvisits AS (
    SELECT
        cast(affiliate_join.browser_id as string) as browser_id,
        affiliate_join.buy_visit_id,
        affiliate_join.receipt_id,
        affiliate_join.visit_id,
        affiliate_join.start_datetime,
        affiliate_join.second_channel,
        affiliate_join.utm_content,
        affiliate_join.run_date,
        affiliate_join.paid,
        affiliate_join.has_referral,
        affiliate_join.external_source,
        affiliate_join.partition_key,
        affiliate_join.from_product_feeds,
        affiliate_join.key_market
      FROM
        affiliate_join
        left join dedupe using (visit_id,receipt_id)
      WHERE dedupe.visit_id is null
    UNION DISTINCT
    SELECT
        buyatt_currvisits.browser_id,
        buyatt_currvisits.buy_visit_id,
        buyatt_currvisits.receipt_id,
        buyatt_currvisits.visit_id,
        buyatt_currvisits.start_datetime,
        buyatt_currvisits.second_channel,
        buyatt_currvisits.utm_content,
        buyatt_currvisits.run_date,
        buyatt_currvisits.paid,
        buyatt_currvisits.has_referral,
        buyatt_currvisits.external_source,
        buyatt_currvisits.partition_key,
        buyatt_currvisits.from_product_feeds,
        buyatt_currvisits.key_market
      FROM
        buyatt_currvisits
  )
  SELECT
      affiliate_currvisits.browser_id,
      affiliate_currvisits.buy_visit_id,
      affiliate_currvisits.receipt_id,
      affiliate_currvisits.visit_id,
      affiliate_currvisits.start_datetime,
      affiliate_currvisits.second_channel,
      affiliate_currvisits.utm_content,
      affiliate_currvisits.run_date,
      affiliate_currvisits.paid,
      affiliate_currvisits.has_referral,
      affiliate_currvisits.external_source,
      affiliate_currvisits.partition_key,
      affiliate_currvisits.from_product_feeds,
      affiliate_currvisits.key_market
    FROM
      affiliate_currvisits
;

 /*
step 5:
THIS IS WHERE WE'RE RE-CALCULATING ATTRIBUTION
 */
CREATE OR REPLACE TEMPORARY TABLE attr_by_browser_slice_pastdat_sans_affiliates
  AS  WITH receipts AS (
    SELECT DISTINCT
        ri.receipt_id,
        ri.buy_date,
        ri.buyer_type,
        ri.buy_visit_id,
        ri.buy_visit_timestamp,
        ri.receipt_timestamp,
        ri.receipt_market,
        ri.browser_id,
        ri.gms,
        ri.gms_gross,
        p.max_visit_before_window
      FROM
        buyatt_awin_receipt_slice AS ri
        LEFT OUTER JOIN buyatt_priorvisits AS p ON ri.buy_visit_id = p.buy_visit_id
  ), visits AS (
    SELECT
        c.buy_visit_id,
        c.receipt_id,
        c.visit_id,
        c.start_datetime,
        c.run_date,
        c.paid,
        c.has_referral,
        c.external_source,
        c.partition_key,
        c.second_channel,
        c.utm_content,
        c.from_product_feeds,
        c.key_market
      FROM
        buyatt_currvisits AS c
  ), factors AS (
    SELECT
        r.buy_date,
        r.buyer_type,
        r.receipt_id,
        r.buy_visit_id,
        r.buy_visit_timestamp,
        r.receipt_timestamp,
        r.gms,
        r.gms_gross,
        r.receipt_market,
        r.max_visit_before_window,
        v.visit_id AS o_visit_id,
        v.start_datetime AS o_visit_timestamp,
        v.run_date AS o_visit_run_date,
        v.partition_key,
        v.second_channel,
        v.utm_content,
        v.from_product_feeds,
        v.key_market,
        CASE
          WHEN (r.max_visit_before_window IS NULL
           OR datetime_diff(Datetime(r.buy_visit_timestamp), Datetime(r.max_visit_before_window), SECOND) > 5184000)
           AND row_number() OVER (PARTITION BY r.receipt_id ORDER BY v.start_datetime, v.visit_id) = 1
           THEN exp(-NUMERIC '0.099' * (UNIX_SECONDS(r.receipt_timestamp) - Unix_seconds(r.buy_visit_timestamp)) / NUMERIC '86400.0')
          ELSE exp(-NUMERIC '0.099' * (UNIX_SECONDS(r.receipt_timestamp) - Unix_seconds(v.start_datetime)) / NUMERIC '86400.0')
        END AS decay_factor,
        --  exp(-0.099*o_visit_timestamp)
        v.paid,
        v.has_referral,
        v.external_source
      FROM
        receipts AS r
        INNER JOIN visits AS v ON r.receipt_id = v.receipt_id
         AND r.buy_visit_id = v.buy_visit_id
  ), factors_with_logic AS (
    SELECT
        factors.buy_date,
        factors.receipt_id,
        factors.receipt_timestamp,
        factors.receipt_market,
        factors.buy_visit_id,
        factors.buyer_type,
        factors.gms,
        factors.gms_gross,
        factors.o_visit_id,
        factors.o_visit_run_date,
        factors.decay_factor,
        factors.paid,
        factors.has_referral,
        factors.external_source,
        --  all channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1 as INT64) AS last_click_all,
        factors.decay_factor / sum(factors.decay_factor) OVER (PARTITION BY factors.receipt_id) AS decay_all,
        --  paid channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id, factors.paid ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1
         AND factors.paid = 1 as INT64) AS paid_last_click_all,
        CASE
          WHEN factors.paid = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END AS paid_decay_factor,
        sum(CASE
          WHEN factors.paid = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END) OVER (PARTITION BY factors.receipt_id) AS paid_decay_factor_sum,
        --  has_referral channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id, factors.has_referral ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1
         AND factors.has_referral = 1 as INT64) AS has_referral_last_click_all,
        CASE
          WHEN factors.has_referral = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END AS has_referral_decay_factor,
        sum(CASE
          WHEN factors.has_referral = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END) OVER (PARTITION BY factors.receipt_id) AS has_referral_decay_factor_sum,
        --  external_source channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id, factors.external_source ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1
         AND factors.external_source = 1 as INT64) AS external_source_last_click_all,
        CASE
          WHEN factors.external_source = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END AS external_source_decay_factor,
        sum(CASE
          WHEN factors.external_source = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END) OVER (PARTITION BY factors.receipt_id) AS external_source_decay_factor_sum,
        factors.partition_key AS visit_partition_key,
        factors.buy_date AS receipt_partition_key,
        factors.second_channel,
        factors.utm_content,
        factors.from_product_feeds,
        factors.key_market
      FROM
        factors
  )
  SELECT
      factors_with_logic.buy_date,
      factors_with_logic.receipt_id,
      factors_with_logic.receipt_timestamp,
      factors_with_logic.receipt_market,
      factors_with_logic.buy_visit_id,
      factors_with_logic.buyer_type,
      factors_with_logic.gms,
      factors_with_logic.gms_gross,
      factors_with_logic.o_visit_id,
      factors_with_logic.o_visit_run_date,
      factors_with_logic.decay_factor,
      factors_with_logic.paid,
      factors_with_logic.has_referral,
      factors_with_logic.external_source,
      --  all channels
      factors_with_logic.last_click_all,
      factors_with_logic.decay_all,
      --  paid
      CASE
        WHEN factors_with_logic.paid_decay_factor_sum > 0 THEN factors_with_logic.paid_last_click_all
        WHEN factors_with_logic.has_referral_decay_factor_sum > 0 THEN factors_with_logic.has_referral_last_click_all
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_last_click_all
        ELSE factors_with_logic.last_click_all
      END AS paid_last_click_all,
      CASE
        WHEN factors_with_logic.paid_decay_factor_sum > 0 THEN factors_with_logic.paid_decay_factor / factors_with_logic.paid_decay_factor_sum
        WHEN factors_with_logic.has_referral_decay_factor_sum > 0 THEN factors_with_logic.has_referral_decay_factor / factors_with_logic.has_referral_decay_factor_sum
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_decay_factor / factors_with_logic.external_source_decay_factor_sum
        ELSE factors_with_logic.decay_all
      END AS paid_decay_all,
      --  has_referral
      CASE
        WHEN factors_with_logic.has_referral_decay_factor_sum > 0 THEN factors_with_logic.has_referral_decay_factor / factors_with_logic.has_referral_decay_factor_sum
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_decay_factor / factors_with_logic.external_source_decay_factor_sum
        ELSE factors_with_logic.decay_all
      END AS has_referral_decay_all,
      --  external_source
      CASE
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_decay_factor / factors_with_logic.external_source_decay_factor_sum
        ELSE factors_with_logic.decay_all
      END AS external_source_decay_all,
      factors_with_logic.visit_partition_key,
      factors_with_logic.receipt_partition_key,
      factors_with_logic.second_channel,
      factors_with_logic.utm_content,
      factors_with_logic.from_product_feeds,
      factors_with_logic.key_market
    FROM
      factors_with_logic
;

CREATE OR REPLACE TEMPORARY TABLE attr_by_browser_slice_pastdat
  AS  WITH receipts AS (
    SELECT DISTINCT
        ri.receipt_id,
        ri.buy_date,
        ri.buyer_type,
        ri.buy_visit_id,
        ri.buy_visit_timestamp,
        ri.receipt_timestamp,
        ri.receipt_market,
        ri.browser_id,
        ri.gms,
        ri.gms_gross,
        p.max_visit_before_window
      FROM
        buyatt_awin_receipt_slice AS ri
        LEFT OUTER JOIN buyatt_priorvisits AS p ON ri.buy_visit_id = p.buy_visit_id
  ), visits AS (
    SELECT
        c.buy_visit_id,
        c.receipt_id,
        c.visit_id,
        c.start_datetime,
        c.run_date,
        c.paid,
        c.has_referral,
        c.external_source,
        c.partition_key,
        c.second_channel,
        c.utm_content,
        c.from_product_feeds,
        c.key_market
      FROM
        affiliate_currvisits AS c
  ), factors AS (
    SELECT
        r.buy_date,
        r.buyer_type,
        r.receipt_id,
        r.buy_visit_id,
        r.buy_visit_timestamp,
        r.receipt_timestamp,
        r.gms,
        r.gms_gross,
        r.receipt_market,
        r.max_visit_before_window,
        v.visit_id AS o_visit_id,
        v.start_datetime AS o_visit_timestamp,
        v.run_date AS o_visit_run_date,
        v.partition_key,
        v.second_channel,
        v.utm_content,
        v.from_product_feeds,
        v.key_market,
        CASE
          WHEN (r.max_visit_before_window IS NULL
           OR datetime_diff(Datetime(r.buy_visit_timestamp), Datetime(r.max_visit_before_window), SECOND) > 5184000)
           AND row_number() OVER (PARTITION BY r.receipt_id ORDER BY v.start_datetime, v.visit_id) = 1
           THEN
          exp(-NUMERIC '0.099' * (UNIX_SECONDS(r.receipt_timestamp) - Unix_seconds(r.buy_visit_timestamp)) / NUMERIC '86400.0')
          ELSE exp(-NUMERIC '0.099' * (UNIX_SECONDS(r.receipt_timestamp) - Unix_seconds(v.start_datetime)) / NUMERIC '86400.0')
        END AS decay_factor,
        --  exp(-0.099*o_visit_timestamp)
        v.paid,
        v.has_referral,
        v.external_source
      FROM
        receipts AS r
        INNER JOIN visits AS v ON r.receipt_id = v.receipt_id
         AND r.buy_visit_id = v.buy_visit_id
  ), factors_with_logic AS (
    SELECT
        factors.buy_date,
        factors.receipt_id,
        factors.receipt_timestamp,
        factors.receipt_market,
        factors.buy_visit_id,
        factors.buyer_type,
        factors.gms,
        factors.gms_gross,
        factors.o_visit_id,
        factors.o_visit_run_date,
        factors.decay_factor,
        factors.paid,
        factors.has_referral,
        factors.external_source,
        --  all channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1 as INT64) AS last_click_all,
        factors.decay_factor / sum(factors.decay_factor) OVER (PARTITION BY factors.receipt_id) AS decay_all,
        --  paid channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id, factors.paid ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1
         AND factors.paid = 1 as INT64) AS paid_last_click_all,
        CASE
          WHEN factors.paid = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END AS paid_decay_factor,
        sum(CASE
          WHEN factors.paid = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END) OVER (PARTITION BY factors.receipt_id) AS paid_decay_factor_sum,
        --  has_referral channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id, factors.has_referral ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1
         AND factors.has_referral = 1 as INT64) AS has_referral_last_click_all,
        CASE
          WHEN factors.has_referral = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END AS has_referral_decay_factor,
        sum(CASE
          WHEN factors.has_referral = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END) OVER (PARTITION BY factors.receipt_id) AS has_referral_decay_factor_sum,
        --  external_source channel logic
        CAST(row_number() OVER (PARTITION BY factors.receipt_id, factors.external_source ORDER BY unix_seconds(CAST(factors.o_visit_timestamp as TIMESTAMP)) DESC) = 1
         AND factors.external_source = 1 as INT64) AS external_source_last_click_all,
        CASE
          WHEN factors.external_source = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END AS external_source_decay_factor,
        sum(CASE
          WHEN factors.external_source = 1 THEN factors.decay_factor
          ELSE CAST(0 as FLOAT64)
        END) OVER (PARTITION BY factors.receipt_id) AS external_source_decay_factor_sum,
        factors.partition_key AS visit_partition_key,
        factors.buy_date AS receipt_partition_key,
        factors.second_channel,
        factors.utm_content,
        factors.from_product_feeds,
        factors.key_market
      FROM
        factors
  )
  SELECT
      factors_with_logic.buy_date,
      factors_with_logic.receipt_id,
      factors_with_logic.receipt_timestamp,
      factors_with_logic.receipt_market,
      factors_with_logic.buy_visit_id,
      factors_with_logic.buyer_type,
      factors_with_logic.gms,
      factors_with_logic.gms_gross,
      factors_with_logic.o_visit_id,
      factors_with_logic.o_visit_run_date,
      factors_with_logic.decay_factor,
      factors_with_logic.paid,
      factors_with_logic.has_referral,
      factors_with_logic.external_source,
      --  all channels
      factors_with_logic.last_click_all,
      factors_with_logic.decay_all,
      --  paid
      CASE
        WHEN factors_with_logic.paid_decay_factor_sum > 0 THEN factors_with_logic.paid_last_click_all
        WHEN factors_with_logic.has_referral_decay_factor_sum > 0 THEN factors_with_logic.has_referral_last_click_all
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_last_click_all
        ELSE factors_with_logic.last_click_all
      END AS paid_last_click_all,
      CASE
        WHEN factors_with_logic.paid_decay_factor_sum > 0 THEN factors_with_logic.paid_decay_factor / factors_with_logic.paid_decay_factor_sum
        WHEN factors_with_logic.has_referral_decay_factor_sum > 0 THEN factors_with_logic.has_referral_decay_factor / factors_with_logic.has_referral_decay_factor_sum
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_decay_factor / factors_with_logic.external_source_decay_factor_sum
        ELSE factors_with_logic.decay_all
      END AS paid_decay_all,
      --  has_referral
      CASE
        WHEN factors_with_logic.has_referral_decay_factor_sum > 0 THEN factors_with_logic.has_referral_decay_factor / factors_with_logic.has_referral_decay_factor_sum
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_decay_factor / factors_with_logic.external_source_decay_factor_sum
        ELSE factors_with_logic.decay_all
      END AS has_referral_decay_all,
      --  external_source
      CASE
        WHEN factors_with_logic.external_source_decay_factor_sum > 0 THEN factors_with_logic.external_source_decay_factor / factors_with_logic.external_source_decay_factor_sum
        ELSE factors_with_logic.decay_all
      END AS external_source_decay_all,
      factors_with_logic.visit_partition_key,
      factors_with_logic.receipt_partition_key,
      factors_with_logic.second_channel,
      factors_with_logic.utm_content,
      factors_with_logic.from_product_feeds,
      factors_with_logic.key_market
    FROM
      factors_with_logic
;

 /*
step 6:
merge re-attributed sales that were underattributed with the attribution of all other visits that occured at least 2 months ago
VALIDATE THAT RE-ATTRIBUTION W/O INCREMENTAL CLICKS MATCHES ORGINAL ATTRIBUTION
 */
CREATE OR REPLACE TEMPORARY TABLE rev_it_up
  AS SELECT
      receipt_id,
      sum(attr_rev) AS attr_rev
    FROM
      `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv`
    GROUP BY 1
;

CREATE OR REPLACE TEMPORARY TABLE attr_by_browser_slice_pastdat_sans_affiliates_rev
  AS  WITH other_receipts AS (
    SELECT
        receipt_id
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser`
      WHERE DATE(CAST(date_trunc(DATE(timestamp_seconds(o_visit_run_date)), MONTH) as TIMESTAMP)) >= date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH)
    EXCEPT DISTINCT
    SELECT
        attr_by_browser_slice_pastdat_sans_affiliates.receipt_id
      FROM
        attr_by_browser_slice_pastdat_sans_affiliates
  ), attr_other_receipts AS (
    SELECT
        att.buy_date,
        att.receipt_id,
        att.receipt_timestamp,
        att.receipt_market,
        att.buy_visit_id,
        att.buyer_type,
        att.gms,
        att.gms_gross,
        att.o_visit_id,
        att.o_visit_run_date,
        att.decay_factor,
        att.paid,
        att.has_referral,
        att.external_source,
        att.last_click_all,
        att.decay_all,
        att.paid_last_click_all,
        att.paid_decay_all,
        att.has_referral_decay_all,
        att.external_source_decay_all,
      FROM
        other_receipts AS a
        INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS att ON a.receipt_id = att.receipt_id
      WHERE DATE(CAST(date_trunc(DATE(timestamp_seconds(att.o_visit_run_date)), MONTH) as TIMESTAMP)) >= date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH)
  ), attr_other_receipts_visits AS (
    SELECT
        a_0.buy_date,
        a_0.receipt_id,
        a_0.receipt_timestamp,
        a_0.receipt_market,
        a_0.buy_visit_id,
        a_0.buyer_type,
        a_0.gms,
        a_0.gms_gross,
        a_0.o_visit_id,
        a_0.o_visit_run_date,
        a_0.decay_factor,
        a_0.paid,
        a_0.has_referral,
        a_0.external_source,
        a_0.last_click_all,
        a_0.decay_all,
        a_0.paid_last_click_all,
        a_0.paid_decay_all,
        a_0.has_referral_decay_all,
        a_0.external_source_decay_all,
        v.second_channel,
        v.utm_content,
        v.key_market,
        CASE
          WHEN v.third_channel in ('affiliates_feed','affiliates_widget') THEN 1
          ELSE 0
        END AS from_product_feeds
      FROM
        attr_other_receipts AS a_0
        INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` AS v ON a_0.o_visit_id = v.visit_id
      WHERE datetime_trunc(CAST(timestamp_seconds(v.run_date) as DATETIME), MONTH) >= CAST(date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH) as DATETIME)
  ), total_attr AS (
    SELECT
        attr_other_receipts_visits.buy_date,
        attr_other_receipts_visits.receipt_id,
        attr_other_receipts_visits.receipt_timestamp,
        attr_other_receipts_visits.receipt_market,
        attr_other_receipts_visits.buy_visit_id,
        attr_other_receipts_visits.buyer_type,
        attr_other_receipts_visits.gms,
        attr_other_receipts_visits.gms_gross,
        attr_other_receipts_visits.o_visit_id,
        attr_other_receipts_visits.o_visit_run_date,
        attr_other_receipts_visits.decay_factor,
        attr_other_receipts_visits.paid,
        attr_other_receipts_visits.has_referral,
        attr_other_receipts_visits.external_source,
        attr_other_receipts_visits.last_click_all,
        attr_other_receipts_visits.decay_all,
        attr_other_receipts_visits.paid_last_click_all,
        attr_other_receipts_visits.paid_decay_all,
        attr_other_receipts_visits.has_referral_decay_all,
        attr_other_receipts_visits.external_source_decay_all,
        attr_other_receipts_visits.second_channel,
        attr_other_receipts_visits.utm_content,
        attr_other_receipts_visits.key_market,
        attr_other_receipts_visits.from_product_feeds
      FROM
        attr_other_receipts_visits
    UNION DISTINCT
    SELECT
        attr_by_browser_slice_pastdat_sans_affiliates_0.buy_date,
        attr_by_browser_slice_pastdat_sans_affiliates_0.receipt_id,
        attr_by_browser_slice_pastdat_sans_affiliates_0.receipt_timestamp,
        attr_by_browser_slice_pastdat_sans_affiliates_0.receipt_market,
        attr_by_browser_slice_pastdat_sans_affiliates_0.buy_visit_id,
        attr_by_browser_slice_pastdat_sans_affiliates_0.buyer_type,
        attr_by_browser_slice_pastdat_sans_affiliates_0.gms,
        attr_by_browser_slice_pastdat_sans_affiliates_0.gms_gross,
        attr_by_browser_slice_pastdat_sans_affiliates_0.o_visit_id,
        attr_by_browser_slice_pastdat_sans_affiliates_0.o_visit_run_date,
        attr_by_browser_slice_pastdat_sans_affiliates_0.decay_factor,
        attr_by_browser_slice_pastdat_sans_affiliates_0.paid,
        attr_by_browser_slice_pastdat_sans_affiliates_0.has_referral,
        attr_by_browser_slice_pastdat_sans_affiliates_0.external_source,
        attr_by_browser_slice_pastdat_sans_affiliates_0.last_click_all,
        attr_by_browser_slice_pastdat_sans_affiliates_0.decay_all,
        attr_by_browser_slice_pastdat_sans_affiliates_0.paid_last_click_all,
        attr_by_browser_slice_pastdat_sans_affiliates_0.paid_decay_all,
        attr_by_browser_slice_pastdat_sans_affiliates_0.has_referral_decay_all,
        attr_by_browser_slice_pastdat_sans_affiliates_0.external_source_decay_all,
        attr_by_browser_slice_pastdat_sans_affiliates_0.second_channel,
        attr_by_browser_slice_pastdat_sans_affiliates_0.utm_content,
        attr_by_browser_slice_pastdat_sans_affiliates_0.key_market,
        attr_by_browser_slice_pastdat_sans_affiliates_0.from_product_feeds
      FROM
        attr_by_browser_slice_pastdat_sans_affiliates AS attr_by_browser_slice_pastdat_sans_affiliates_0
  )
  SELECT
      r.buy_date,
      r.receipt_id,
      r.receipt_timestamp,
      r.receipt_market,
      r.buy_visit_id,
      r.buyer_type,
      r.gms,
      r.gms_gross,
      r.o_visit_id,
      r.o_visit_run_date,
      r.decay_factor,
      r.paid,
      r.has_referral,
      r.external_source,
      r.last_click_all,
      r.decay_all,
      r.paid_last_click_all,
      r.paid_decay_all,
      r.has_referral_decay_all,
      r.external_source_decay_all,
      r.second_channel,
      r.utm_content,
      r.from_product_feeds,
      r.key_market,
      rev.attr_rev
    FROM
      total_attr AS r
      LEFT OUTER JOIN rev_it_up AS rev USING (receipt_id)
;

CREATE OR REPLACE TEMPORARY TABLE attr_by_browser_slice_pastdat_rev
  AS  WITH other_receipts AS (
    SELECT
        receipt_id
      FROM
        `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser`
      WHERE DATE(CAST(date_trunc(DATE(timestamp_seconds(o_visit_run_date)), MONTH) as TIMESTAMP)) >= date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH)
    EXCEPT DISTINCT
    SELECT
        attr_by_browser_slice_pastdat.receipt_id
      FROM
        attr_by_browser_slice_pastdat
  ), attr_other_receipts AS (
    SELECT
        att.buy_date,
        att.receipt_id,
        att.receipt_timestamp,
        att.receipt_market,
        att.buy_visit_id,
        att.buyer_type,
        att.gms,
        att.gms_gross,
        att.o_visit_id,
        att.o_visit_run_date,
        att.decay_factor,
        att.paid,
        att.has_referral,
        att.external_source,
        att.last_click_all,
        att.decay_all,
        att.paid_last_click_all,
        att.paid_decay_all,
        att.has_referral_decay_all,
        att.external_source_decay_all
      FROM
        other_receipts AS a
        INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS att ON a.receipt_id = att.receipt_id
      WHERE DATE(CAST(date_trunc(DATE(timestamp_seconds(att.o_visit_run_date)), MONTH) as TIMESTAMP)) >= date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH)
  ), attr_other_receipts_visits AS (
    SELECT
        a_0.buy_date,
        a_0.receipt_id,
        a_0.receipt_timestamp,
        a_0.receipt_market,
        a_0.buy_visit_id,
        a_0.buyer_type,
        a_0.gms,
        a_0.gms_gross,
        a_0.o_visit_id,
        a_0.o_visit_run_date,
        a_0.decay_factor,
        a_0.paid,
        a_0.has_referral,
        a_0.external_source,
        a_0.last_click_all,
        a_0.decay_all,
        a_0.paid_last_click_all,
        a_0.paid_decay_all,
        a_0.has_referral_decay_all,
        a_0.external_source_decay_all,
        v.second_channel,
        v.utm_content,
        v.key_market,
        CASE
          WHEN v.third_channel in ('affiliates_feed', 'affiliates_widget') THEN 1
          ELSE 0
        END AS from_product_feeds
      FROM
        attr_other_receipts AS a_0
        INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` AS v ON a_0.o_visit_id = v.visit_id
      WHERE datetime_trunc(CAST(timestamp_seconds(v.run_date) as DATETIME), MONTH) >= CAST(date_sub(DATE(CAST(date_trunc(current_date(), MONTH) as TIMESTAMP)), interval 48 MONTH) as DATETIME)
  ), total_attr AS (
    SELECT
        attr_other_receipts_visits.buy_date,
        attr_other_receipts_visits.receipt_id,
        attr_other_receipts_visits.receipt_timestamp,
        attr_other_receipts_visits.receipt_market,
        attr_other_receipts_visits.buy_visit_id,
        attr_other_receipts_visits.buyer_type,
        attr_other_receipts_visits.gms,
        attr_other_receipts_visits.gms_gross,
        attr_other_receipts_visits.o_visit_id,
        attr_other_receipts_visits.o_visit_run_date,
        attr_other_receipts_visits.decay_factor,
        attr_other_receipts_visits.paid,
        attr_other_receipts_visits.has_referral,
        attr_other_receipts_visits.external_source,
        attr_other_receipts_visits.last_click_all,
        attr_other_receipts_visits.decay_all,
        attr_other_receipts_visits.paid_last_click_all,
        attr_other_receipts_visits.paid_decay_all,
        attr_other_receipts_visits.has_referral_decay_all,
        attr_other_receipts_visits.external_source_decay_all,
        attr_other_receipts_visits.second_channel,
        attr_other_receipts_visits.utm_content,
        attr_other_receipts_visits.key_market,
        attr_other_receipts_visits.from_product_feeds
      FROM
        attr_other_receipts_visits
    UNION DISTINCT
    SELECT
        attr_by_browser_slice_pastdat_0.buy_date,
        attr_by_browser_slice_pastdat_0.receipt_id,
        attr_by_browser_slice_pastdat_0.receipt_timestamp,
        attr_by_browser_slice_pastdat_0.receipt_market,
        attr_by_browser_slice_pastdat_0.buy_visit_id,
        attr_by_browser_slice_pastdat_0.buyer_type,
        attr_by_browser_slice_pastdat_0.gms,
        attr_by_browser_slice_pastdat_0.gms_gross,
        attr_by_browser_slice_pastdat_0.o_visit_id,
        attr_by_browser_slice_pastdat_0.o_visit_run_date,
        attr_by_browser_slice_pastdat_0.decay_factor,
        attr_by_browser_slice_pastdat_0.paid,
        attr_by_browser_slice_pastdat_0.has_referral,
        attr_by_browser_slice_pastdat_0.external_source,
        attr_by_browser_slice_pastdat_0.last_click_all,
        attr_by_browser_slice_pastdat_0.decay_all,
        attr_by_browser_slice_pastdat_0.paid_last_click_all,
        attr_by_browser_slice_pastdat_0.paid_decay_all,
        attr_by_browser_slice_pastdat_0.has_referral_decay_all,
        attr_by_browser_slice_pastdat_0.external_source_decay_all,
        attr_by_browser_slice_pastdat_0.second_channel,
        attr_by_browser_slice_pastdat_0.utm_content,
        attr_by_browser_slice_pastdat_0.key_market,
        attr_by_browser_slice_pastdat_0.from_product_feeds
      FROM
        attr_by_browser_slice_pastdat AS attr_by_browser_slice_pastdat_0
  )
  SELECT
      r.buy_date,
      r.receipt_id,
      r.receipt_timestamp,
      r.receipt_market,
      r.buy_visit_id,
      r.buyer_type,
      r.gms,
      r.gms_gross,
      r.o_visit_id,
      r.o_visit_run_date,
      r.decay_factor,
      r.paid,
      r.has_referral,
      r.external_source,
      r.last_click_all,
      r.decay_all,
      r.paid_last_click_all,
      r.paid_decay_all,
      r.has_referral_decay_all,
      r.external_source_decay_all,
      r.second_channel,
      r.utm_content,
      r.key_market,
      r.from_product_feeds,
      rev.attr_rev
    FROM
      total_attr AS r
      LEFT OUTER JOIN rev_it_up AS rev USING (receipt_id)
;

create temp table affiliate_tactics as 
  (select distinct a.utm_content as publisher_id,
    case when (b.publisher_id is not null or c.publisher_id is not null or t.tactic = 'Social Creator Co') and a.utm_content = '946733' then "Social - CreatorIQ" 
    when (b.publisher_id is not null or c.publisher_id is not null or t.tactic = 'Social Creator Co') and a.utm_content <> '946733' then "Social" 
    when t.tactic is null and b.publisher_id is null and c.publisher_id is null then 'NA'
    else t.tactic end as tactic
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` t on a.utm_content = t.publisher_id
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic_tiktok` b on a.utm_content = b.publisher_id
    left join (select distinct publisher_id from `etsy-data-warehouse-prod.rollups.influencer_cc_overview`) c on a.utm_content = c.publisher_id
    where channel_group = 'Affiliates'
    union distinct
    select distinct  cast(a.publisher_id as string)  as publisher_id,
    case when (b.publisher_id is not null or c.publisher_id is not null or t.tactic = 'Social Creator Co') and cast(a.publisher_id as string) = '946733' then "Social - CreatorIQ" 
    when (b.publisher_id is not null or c.publisher_id is not null or t.tactic = 'Social Creator Co') and cast(a.publisher_id as string) <> '946733' then "Social" 
    when t.tactic is null and b.publisher_id is null and c.publisher_id is null then 'NA'
    else t.tactic end as tactic
    from `etsy-data-warehouse-prod.marketing.awin_spend_data` a
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` t on cast(a.publisher_id as string) = t.publisher_id
    left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic_tiktok` b on cast(a.publisher_id as string) = b.publisher_id
    left join (select distinct publisher_id from `etsy-data-warehouse-prod.rollups.influencer_cc_overview`) c on cast(a.publisher_id as string) = c.publisher_id);

create or replace table `etsy-data-warehouse-dev.tnormil.granular_awin_content_attribution_click`  as 
(with neww as
(SELECT date(timestamp_seconds(o_visit_run_date)) as date, key_market, tactic, sum(	external_source_decay_all	) as 	external_source_decay_all,
count(distinct o_visit_id) as visits_new
FROM attr_by_browser_slice_pastdat_rev a
left join affiliate_tactics r on a.utm_content = r.publisher_id
where second_channel = 'affiliates'
group by 1,2,3),
oldd as
(SELECT date(timestamp_seconds(o_visit_run_date)) as date, key_market, tactic, sum(	external_source_decay_all	) as 	external_source_decay_all
FROM attr_by_browser_slice_pastdat_sans_affiliates_rev a
left join affiliate_tactics r on a.utm_content = r.publisher_id
where second_channel = 'affiliates'
group by 1,2,3),
db as
(select n.date,
n,key_market, 
n.tactic,
coalesce(o.external_source_decay_all,0) as external_source_decay_all, coalesce(n.external_source_decay_all,0) as external_source_decay_all_new, 
visits_new
from neww n
left join oldd o using (date, key_market, tactic))
select date, key_market, tactic,
sum(external_source_decay_all) as external_source_decay_all,
sum(external_source_decay_all_new) as external_source_decay_all_new
from db
group by 1,2,3);

END;
