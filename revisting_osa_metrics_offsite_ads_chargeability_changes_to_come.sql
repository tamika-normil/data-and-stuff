#for chargeable aov, aov, and yoy metrics

BEGIN
--  Daily rollup with chargeability by channel, buyer country, category, and seller tier
--  Feature list here: https://docs.google.com/spreadsheets/d/1fzO8wJ-9FEAOjy_lGHOXc3rC8gkiSf4FSKwQe_uSby4/edit#gid=117826287
--  Owner: mthorn@etsy.com
--  Dependencies:
--  rollups.seller_basics
--  buyatt_mart.buyatt_analytics
--  rollups.active_sellers_rollup_user_ids_12m
--  transaction_mart.transactions_gms_by_trans
--  first bring in attributed gms per receipt along with key channel/category data

CREATE TEMPORARY TABLE attr_visits
  AS SELECT
      v.visit_id,
      v.receipt_id,
      v.visit_date,
      v.order_date,
      v.top_channel,
      v.second_channel,
      v.third_channel,
      v.utm_campaign,
      v.utm_medium,
      v.category,
      v.channel_int,
      v.canonical_region,
      v.mapped_region,
      v.attr_gms,
      v.attr_receipt,
      v.attr_rev,
      v.gms,
      v.landing_listing_id,
      v.device,
      row_number() OVER (PARTITION BY v.receipt_id, v.channel_int ORDER BY v.visit_id DESC) AS order_channel_rank
    FROM
      (
        SELECT
            b.visit_id,
            a.receipt_id,
            DATE(timestamp_seconds(a.o_visit_run_date)) AS visit_date,
            DATE(a.receipt_timestamp) AS order_date,
            --  channel and category dimensions
            b.top_channel,
            b.second_channel,
            b.third_channel,
            case when b.second_channel in ('affiliates') then b.third_channel else substr(coalesce(lower(b.utm_campaign),''),1,120) end as utm_campaign,
            CASE
              WHEN b.second_channel IN(
                'facebook_disp', 'pinterest_disp', 'instagram_disp', 'facebook_disp_intl'
              ) THEN CASE
                WHEN upper(b.utm_campaign) LIKE '%_CUR_%' THEN 'curated'
                ELSE 'non-curated'
              END
              ELSE coalesce(regexp_extract(lower(b.utm_campaign), 'accessories|art_collectibles|bags_and_purses|bath_beauty|books_movies_music|clothing|craft_supplies|electronics|home_and_living|jewelry|not-pla|other|paper_party|pet_supplies|shoes|showcase|toys_and_games|wedding|art_and_collectibles|paper_goods|paper_and_party_supplies', 1, 1), 'other')
            END AS category,
            CASE
              WHEN b.second_channel IN(
                'gpla', 'intl_gpla'
              ) THEN 1
              WHEN b.second_channel IN(
                'facebook_disp', 'facebook_disp_intl'
              ) THEN 2
              WHEN b.second_channel = 'instagram_disp' THEN 3
              WHEN b.second_channel IN(
                'bing_plas', 'intl_bing_plas'
              ) THEN 4
              WHEN b.second_channel in ('pinterest_disp','pinterest_disp_intl') THEN 5
	            WHEN b.second_channel = 'affiliates' and (b.third_channel = 'affiliates_feed' or b.third_channel = 'affiliates_widget' ) THEN 6		
              WHEN b.second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%' then 7
              ELSE 0
            END AS channel_int,
            b.mapped_platform_type as device,
            --  region
            b.canonical_region,
            CASE
              WHEN b.canonical_region IN(
                'US', 'GB', 'CA', 'DE', 'FR', 'AU'
              ) THEN b.canonical_region
              ELSE 'ROW'
            END AS mapped_region,
            b.utm_medium,
            --  attr and total receipt GMS for the chargeability calcs
            a.external_source_decay_all AS attr_receipt,
            CAST(a.external_source_decay_all * a.gms as NUMERIC) AS attr_gms,
            CAST(a.external_source_decay_all * clv.attr_rev as NUMERIC) AS attr_rev,
            a.gms,
            coalesce(CAST(regexp_extract(b.landing_event_url, '(\?i)listing\\/(\\d{1,9})', 1, 1) as INT64), -1) AS landing_listing_id
         FROM
           `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` as a
         INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` as b
         ON a.o_visit_run_date = b.run_date and a.o_visit_id = b.visit_id
         INNER join `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` clv on a.receipt_id = clv.receipt_id
         WHERE a.receipt_timestamp >= TIMESTAMP '2020-02-04 00:00:00' -- when we began writing USD acquisition fees reliably
          AND a.receipt_timestamp < CAST(CAST(current_date() as DATETIME) AS TIMESTAMP)
          AND a.o_visit_run_date >= UNIX_SECONDS(CAST(CAST(DATE '2020-01-04' as DATETIME) AS TIMESTAMP))
                  AND a.o_visit_run_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
                  AND b._date >= '2020-01-04'
                  AND b._date < current_date()
          AND (b.second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates'
                   ) or (b.second_channel in ('native_display','intl_native_display') and third_channel not like '%msan%'))
          AND upper(b.utm_campaign) NOT LIKE '%_CUR_%'
       ORDER BY
         --  remove curated from this
         1 NULLS LAST,
         2
     ) AS v
;

--  grab utm_custom2 from weblog.visits
CREATE TEMPORARY TABLE custom_utms
 AS SELECT
     visits.visit_id,
     substr(visits.utm_custom2,1,100) as utm_custom2
   FROM
    `etsy-data-warehouse-prod.weblog.visits` as visits
   WHERE visits._date >= '2020-01-01'
    AND visits.visit_id IN(
     SELECT
         attr_visits.visit_id
       FROM
         attr_visits
   )
;

--  connect landing listings to shops
CREATE TEMPORARY TABLE landing_shops
 AS SELECT DISTINCT
     a.visit_id,
     a.visit_date,
     l.user_id
   FROM
     attr_visits AS a
     INNER JOIN `etsy-data-warehouse-prod.listing_mart.listings` AS l ON a.landing_listing_id = l.listing_id
;

--  inner joining will exclude nulls, will have to coalesce below
--  create temp table with seller tier by month for interpolated join
--  limit to first day of month to reduce size
CREATE TEMPORARY TABLE monthly_tiers
 AS  WITH trailing_gms AS (
   SELECT
       r.date,
       r.user_id,
       sum(g.gms_net) AS past_year_gms
     FROM
       `etsy-data-warehouse-prod.rollups.active_sellers_rollup_user_ids_12m` AS r
       INNER JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` AS g ON r.user_id = g.seller_user_id
        AND g.date BETWEEN date_sub(r.date, interval 365 DAY) AND date_sub(r.date, interval 1 DAY)
     WHERE r.date >= DATE '2020-01-01'
            AND extract(DAY from CAST(r.date as DATETIME)) = 1
      GROUP BY 1, 2
    ORDER BY
      1,
      2
  )
  SELECT
      trailing_gms.date,
      trailing_gms.user_id,
      trailing_gms.past_year_gms,
      CASE
        WHEN trailing_gms.past_year_gms >= 50000 THEN 'power_seller'
        WHEN trailing_gms.past_year_gms >= 5000 THEN 'top_seller'
        ELSE 'non_top_seller'
      END AS seller_tier,
      lead(trailing_gms.date, 1) OVER (PARTITION BY trailing_gms.user_id ORDER BY trailing_gms.date) - 1 AS thru_date
    FROM
      trailing_gms
;
--  now join seller tiers to landing shops
CREATE TEMPORARY TABLE landing_shop_tiers
  AS  WITH sb_dedupe AS (
    SELECT DISTINCT
        seller_basics.user_id,
        CASE
          WHEN seller_basics.seller_tier = 'power seller' THEN 'power_seller'
          WHEN seller_basics.seller_tier = 'top seller' THEN 'top_seller'
          ELSE 'non_top_seller'
        END AS seller_tier
      FROM
        `etsy-data-warehouse-prod.rollups.seller_basics` as seller_basics
      WHERE seller_basics.user_id IN(
        SELECT
            landing_shops.user_id
          FROM
            landing_shops
      )
    ORDER BY
      1
  ), l_s AS (
    SELECT
        row_number() OVER (ORDER BY unix_date(l.visit_date)) AS cw_rn,
        l.visit_id,
        l.visit_date,
        l.user_id,
        s.user_id AS user_id_0,
        s.seller_tier
      FROM
        landing_shops AS l
        INNER JOIN sb_dedupe AS s ON l.user_id = s.user_id
  ), m AS (
    SELECT
        row_number() OVER (ORDER BY unix_date(m.date)) AS cw_rn,
        m.date,
        m.user_id,
        m.past_year_gms,
        m.seller_tier
      FROM
        monthly_tiers AS m
  )
  SELECT
      cw_intp.l_s_visit_id AS visit_id,
      cw_intp.l_s_visit_date AS visit_date,
      cw_intp.l_s_user_id AS user_id,
      coalesce(cw_intp.m_seller_tier, cw_intp.l_s_seller_tier) AS seller_tier
    FROM
      (
        SELECT
            cw_intp_join.cw_intp_lhs,
            cw_intp_join.cw_intp_rhs,
            cw_intp_join.cw_intp_rn,
            cw_intp_join.l_s_cw_rn,
            cw_intp_join.l_s_visit_id,
            cw_intp_join.l_s_visit_date,
            cw_intp_join.l_s_user_id,
                       cw_intp_join.l_s_user_id AS l_s_user_id_0,
           cw_intp_join.l_s_seller_tier,
           cw_intp_join.m_cw_rn,
           cw_intp_join.m_date,
           cw_intp_join.m_user_id,
           cw_intp_join.m_past_year_gms,
           cw_intp_join.m_seller_tier
         FROM
           (
             SELECT
                 l_s.visit_date AS cw_intp_lhs,
                 m_0.date AS cw_intp_rhs,
                 CASE
                   WHEN m_0.date IS NOT NULL THEN row_number() OVER (PARTITION BY l_s.user_id, l_s.visit_date, l_s.cw_rn ORDER BY m_0.date DESC, m_0.cw_rn)
                   ELSE 1
                 END AS cw_intp_rn,
                 l_s.cw_rn AS l_s_cw_rn,
                 l_s.visit_id AS l_s_visit_id,
                 l_s.visit_date AS l_s_visit_date,
                 l_s.user_id AS l_s_user_id,
                 l_s.user_id AS l_s_user_id_0,
                 l_s.seller_tier AS l_s_seller_tier,
                 m_0.cw_rn AS m_cw_rn,
                 m_0.date AS m_date,
                 m_0.user_id AS m_user_id,
                 m_0.past_year_gms AS m_past_year_gms,
                 m_0.seller_tier AS m_seller_tier
               FROM
                 l_s
                 LEFT OUTER JOIN m AS m_0 ON l_s.user_id = m_0.user_id
                  AND l_s.visit_date >= m_0.date
           ) AS cw_intp_join
         WHERE cw_intp_join.cw_intp_rn = 1
          OR cw_intp_join.cw_intp_lhs = cw_intp_join.cw_intp_rhs
     ) AS cw_intp
;

--  now bring it all together!
CREATE TEMP TABLE offsite_ads_chargeability_temp
 AS (SELECT
     a.order_date,
     a.visit_date,
     a.top_channel,
     a.second_channel,
     a.third_channel,
     a.utm_campaign,
     a.utm_medium, 
     c.utm_custom2,
     a.category,
     a.canonical_region,
     a.mapped_region,
     a.device,
     coalesce(t.seller_tier, 'unknown') AS seller_tier,
     coalesce(sum(a.attr_gms), CAST(0 as NUMERIC)) AS attr_gms,
     coalesce(sum(a.attr_receipt), CAST(0 as NUMERIC)) AS attr_receipt,
     coalesce(sum(a.attr_rev), CAST(0 as NUMERIC)) AS attr_rev,    
     coalesce(sum(CASE
       WHEN r.receipt_id IS NOT NULL
        AND a.order_channel_rank = 1 THEN r.acquisition_fee_usd / 100
     END), CAST(0 as NUMERIC)) AS advertising_revenue,
     coalesce(sum(CASE
       WHEN r.receipt_id IS NOT NULL
        AND a.order_channel_rank = 1 THEN a.gms
     END), CAST(0 as NUMERIC)) AS chargeable_gms,
     coalesce(count(CASE
       WHEN r.receipt_id IS NOT NULL
        AND a.order_channel_rank = 1 THEN r.receipt_id
     END), CAST(0 as NUMERIC)) AS chargeable_receipts,
   FROM
     attr_visits AS a
     INNER JOIN custom_utms AS c ON a.visit_id = c.visit_id
     LEFT OUTER JOIN landing_shop_tiers AS t ON a.visit_id = t.visit_id
     LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON a.receipt_id = r.receipt_id
      AND a.channel_int = r.channel
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);


create temp table keys
partition by `order_date`
cluster by canonical_region as (
 select distinct order_date,
     visit_date,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
from offsite_ads_chargeability_temp
union distinct 
select distinct date_add(`order_date`, interval 1 year) as order_date,
     date_add(`visit_date`, interval 1 year) as visit_date,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
from offsite_ads_chargeability_temp
where date_add(`order_date`, interval 1 year) < current_date
union distinct 
select  distinct date_add(`order_date`, interval 52 week) as order_date,
     date_add(`visit_date`, interval 52 week) as visit_date,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
from offsite_ads_chargeability_temp
where date_add(`order_date`, interval 52 week) < current_date
union distinct 
select distinct date_add(`order_date`, interval 1 week) as order_date,
     date_add(`visit_date`, interval 1 week) as visit_date,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
from offsite_ads_chargeability_temp
where date_add(`order_date`, interval 1 week) < current_date
union distinct 
select distinct date_add(`order_date`, interval 104 week) as order_date,
     date_add(`visit_date`, interval 104 week) as visit_date,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
from offsite_ads_chargeability_temp
where date_add(`order_date`, interval 104 week) < current_date
union distinct 
select distinct date_add(`order_date`, interval 156 week) as order_date,
     date_add(`visit_date`, interval 156 week) as visit_date,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
from offsite_ads_chargeability_temp
where date_add(`order_date`, interval 156 week) < current_date);   


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability` as (
select k.order_date,
     k.visit_date,
     k.top_channel,
     k.second_channel,
     k.third_channel,
     k.utm_campaign,
     k.utm_medium, 
     k.utm_custom2,
     k.category,
     k.canonical_region,
     k.mapped_region,
     k.device,
     k.seller_tier,
     -- THIS YEAR
     a.attr_gms,
     a.attr_receipt,
     a.attr_rev,    
     a.advertising_revenue,
     a.chargeable_gms,
     a.chargeable_receipts,
      -- LAST YEAR
     b.attr_gms as attr_gms_ly,
     b.attr_receipt as attr_receipt_ly,
     b.attr_rev as attr_rev_ly,   
     b.advertising_revenue as advertising_revenue_ly, 
     b.chargeable_gms as chargeable_gms_ly,
     b.chargeable_receipts as chargeable_receipts_ly,
      -- LAST WEEK
     c.attr_gms as attr_gms_lw,
     c.attr_receipt as attr_receipt_lw,
     c.attr_rev as attr_rev_lw,   
     c.advertising_revenue as advertising_revenue_lw, 
     c.chargeable_gms as chargeable_gms_lw,
     c.chargeable_receipts as chargeable_receipts_lw, 
      -- SAME DAY LAST YEAR
     d.attr_gms as attr_gms_dly,
     d.attr_receipt as attr_receipt_dly,
     d.attr_rev as attr_rev_dly,   
     d.advertising_revenue as advertising_revenue_dly, 
     d.chargeable_gms as chargeable_gms_dly,
     d.chargeable_receipts as chargeable_receipts_dly, 
      -- SAME DAY 2 YEARS AGO
     e.attr_gms as attr_gms_dlly,
     e.attr_receipt as attr_receipt_dlly,
     e.attr_rev as attr_rev_dlly,   
     e.advertising_revenue as advertising_revenue_dlly, 
     e.chargeable_gms as chargeable_gms_dlly,
     e.chargeable_receipts as chargeable_receipts_dlly,
      -- SAME DAY 3 YEARS AGO
     f.attr_gms as attr_gms_d3ly,
     f.attr_receipt as attr_receipt_d3ly,
     f.attr_rev as attr_rev_d3ly,   
     f.advertising_revenue as advertising_revenue_d3ly, 
     f.chargeable_gms as chargeable_gms_d3ly,
     f.chargeable_receipts as chargeable_receipts_d3ly,
from etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.keys k
left join etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.offsite_ads_chargeability_temp a
on k.`order_date` = a.`order_date`
  and k.`visit_date` = a.`visit_date`
  and k.top_channel = a.top_channel
  and k.second_channel = a.second_channel
  and k.third_channel = a.third_channel
  and k.utm_campaign = a.utm_campaign
  and k.utm_medium = a.utm_medium
  and k.utm_custom2 = a.utm_custom2
  and k.category = a.category
  and k.canonical_region = a.canonical_region
  and k.mapped_region = a.mapped_region
  and k.device = a.device
  and k.seller_tier = a.seller_tier
left join  (
  SELECT
    date_add(order_date, interval 1 year) AS order_date1year,
    date_add(visit_date, interval 1 year) AS visit_date1year,
     top_channel,
     second_channel,
     third_channel,
     utm_campaign,
     utm_medium, 
     utm_custom2,
     category,
     canonical_region,
     mapped_region,
     device,
     seller_tier,
     sum(attr_gms) AS attr_gms,
     sum(attr_receipt) AS attr_receipt,
     sum(attr_rev) AS attr_rev,    
     sum(advertising_revenue) AS advertising_revenue,
     sum(chargeable_gms) as chargeable_gms,
     sum(chargeable_receipts) as chargeable_receipts,
     FROM etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.offsite_ads_chargeability_temp
     GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) b
on k.`order_date` = b.order_date1year
  and k.`visit_date` = b.visit_date1year
  and k.top_channel = b.top_channel
  and k.second_channel = b.second_channel
  and k.third_channel = b.third_channel
  and k.utm_campaign = b.utm_campaign
  and k.utm_medium = b.utm_medium
  and k.utm_custom2 = b.utm_custom2
  and k.category = b.category
  and k.canonical_region = b.canonical_region
  and k.mapped_region = b.mapped_region
  and k.device = b.device
  and k.seller_tier = b.seller_tier
left join etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.offsite_ads_chargeability_temp c
on k.`order_date` = date_add(c.`order_date`, interval 7 day)
  and k.`visit_date` = date_add(c.`visit_date`, interval 7 day) 
  and k.top_channel = c.top_channel
  and k.second_channel = c.second_channel
  and k.third_channel = c.third_channel
  and k.utm_campaign = c.utm_campaign
  and k.utm_medium = c.utm_medium
  and k.utm_custom2 = c.utm_custom2
  and k.category = c.category
  and k.canonical_region = c.canonical_region
  and k.mapped_region = c.mapped_region
  and k.device = c.device
  and k.seller_tier = c.seller_tier
  and c.`order_date` < date_sub(date_sub(current_date, interval 7 day), interval 1 day)
left join etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.offsite_ads_chargeability_temp d
on k.`order_date` = date_add(d.`order_date`, interval 52 week)
  and k.`visit_date` = date_add(d.`visit_date`, interval 52 week) 
  and k.top_channel = d.top_channel
  and k.second_channel = d.second_channel
  and k.third_channel = d.third_channel
  and k.utm_campaign = d.utm_campaign
  and k.utm_medium = d.utm_medium
  and k.utm_custom2 = d.utm_custom2
  and k.category = d.category
  and k.canonical_region = d.canonical_region
  and k.mapped_region = d.mapped_region
  and k.device = d.device
  and k.seller_tier = d.seller_tier
  and d.`order_date` < date_sub(date_sub(current_date, interval 52 week), interval 1 day)
left join etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.offsite_ads_chargeability_temp e
on k.`order_date` = date_add(e.`order_date`,  interval 104 week)
  and k.`visit_date` = date_add(e.`visit_date`,  interval 104 week) 
  and k.top_channel = e.top_channel
  and k.second_channel = e.second_channel
  and k.third_channel = e.third_channel
  and k.utm_campaign = e.utm_campaign
  and k.utm_medium = e.utm_medium
  and k.utm_custom2 = e.utm_custom2
  and k.category = e.category
  and k.canonical_region = e.canonical_region
  and k.mapped_region = e.mapped_region
  and k.device = e.device
  and k.seller_tier = e.seller_tier
  and e.`order_date` < date_sub(date_sub(current_date,  interval 104 week), interval 1 day) 
left join etsy-bigquery-adhoc-prod._scriptdc5d793e72164b104e5d6cd9ddf32ddc3ee62df2.offsite_ads_chargeability_temp f
on k.`order_date` = date_add(f.`order_date`,  interval 156 week)
  and k.`visit_date` = date_add(f.`visit_date`,  interval 156 week) 
  and k.top_channel = f.top_channel
  and k.second_channel = f.second_channel
  and k.third_channel = f.third_channel
  and k.utm_campaign = f.utm_campaign
  and k.utm_medium = f.utm_medium
  and k.utm_custom2 = f.utm_custom2
  and k.category = f.category
  and k.canonical_region = f.canonical_region
  and k.mapped_region = f.mapped_region
  and k.device = f.device
  and k.seller_tier = f.seller_tier
  and f.`order_date` < date_sub(date_sub(current_date,  interval 156 week), interval 1 day));

END;
