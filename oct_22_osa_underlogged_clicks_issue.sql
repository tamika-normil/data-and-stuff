/*
create temp table visits as  
(SELECT distinct visit_id 
FROM
  `etsy-visit-pipe-prod.canonical.visit_id_beacons`
WHERE
  DATE(_PARTITIONTIME) >= "2022-09-11"
  AND beacon.loc LIKE '%{gclid}%');
  */

create temp table visits as  
(SELECT distinct visit_id
FROM
  `etsy-data-warehouse-prod.rollups.osaa_under_logged_clicks`
WHERE
  click_date_trunc>="2022-01-01"
  and url like '%{gclid}%');

select top_channel, second_channel, third_channel, case when osa.visit_id is not null then 1 else 0 end as osa_eligible, count(distinct b.visit_id) as visits
from `etsy-data-warehouse-prod.buyatt_mart.visits` as b
        join visits v on b.visit_id = v.visit_id
        left join `etsy-data-warehouse-prod.rollups.osa_click_to_visit_join` osa on b.visit_id = osa.visit_id
        where _date >= '2022-01-01'
       group by 1,2,3,4
       order by 4 desc;

select top_channel, second_channel, third_channel, marketing_region, utm_campaign, count(distinct b.visit_id) as visits
from `etsy-data-warehouse-prod.buyatt_mart.visits` as b
        join visits v on b.visit_id = v.visit_id
        left join `etsy-data-warehouse-prod.rollups.osa_click_to_visit_join` osa on b.visit_id = osa.visit_id
where osa.visit_id is not null
          group by 1,2,3,4,5
       order by 6 desc;

select date_trunc(osa.click_date, month) as click_month, count(distinct osa.visit_id) as visits, count(distinct v.visit_id) as err_visits, count(distinct case when ads.click_id is not null then osa.visit_id end) as cvr_visits
from  `etsy-data-warehouse-prod.rollups.osa_click_to_visit_join` osa 
left join visits v using (visit_id)
left join `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` ads using (click_id)
          group by 1;

select sum(chargeable_gms)/sum(attr_gms) as chargeability, 'chargeability_wo_error_clicks' as label
from `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability`
where visit_date >= "2022-01-01"
and flag <> 'error'
union all 
select sum(chargeable_gms)/sum(attr_gms) as chargeability, 'bau_chargeability_wo_error_clicks'
from `etsy-data-warehouse-prod.rollups.offsite_ads_chargeability`
where visit_date >= "2022-01-01";

-- code for referenced offsite ads table is below

-- owner: mthorn@etsy.com, vbhuta@etsy.com, tnormil@etsy.com, annabradleywebb@etsy.com 
-- owner_team: marketinganalytics@etsy.com
-- description: Daily rollup with chargeability by channel, buyer country, category, and seller tier
-- dependencies: etsy-data-warehouse-prod.rollups.seller_basics
-- dependencies: etsy-data-warehouse-prod.rollups.active_sellers_rollup_user_ids_12m
-- dependencies: etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans

-- Notes below:
-- Feature list here: https://docs.google.com/spreadsheets/d/1fzO8wJ-9FEAOjy_lGHOXc3rC8gkiSf4FSKwQe_uSby4/edit#gid=117826287
-- first bring in attributed gms per receipt along with key channel/category data

BEGIN

create temp table visits as  
(SELECT distinct visit_id
FROM
  `etsy-data-warehouse-prod.rollups.osaa_under_logged_clicks`
WHERE
  click_date_trunc>="2022-01-01"
  and url like '%{gclid}%');

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
      v.gms,
      v.landing_listing_id,
      v.device,
      v.flag,
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
              when b.second_channel in ('intl_css_plas') then 8
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
            a.gms,
            coalesce(CAST(regexp_extract(b.landing_event_url, '(\?i)listing\\/(\\d{1,9})', 1, 1) as INT64), -1) AS landing_listing_id,
            case when v.visit_id is not null then 'error' else 'okay' end as flag
         FROM
           `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` as a
         INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` as b
         ON a.o_visit_run_date = b.run_date and a.o_visit_id = b.visit_id
        left join visits v on b.visit_id = v.visit_id
         WHERE a.receipt_timestamp >= TIMESTAMP '2020-02-04 00:00:00' -- when we began writing USD acquisition fees reliably
          AND a.receipt_timestamp < CAST(CAST(current_date() as DATETIME) AS TIMESTAMP)
          AND a.o_visit_run_date >= UNIX_SECONDS(CAST(CAST(DATE '2020-01-04' as DATETIME) AS TIMESTAMP))
                  AND a.o_visit_run_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
                  AND b._date >= '2020-01-04'
                  AND b._date < current_date()
          AND (b.second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'pinterest_disp_intl','instagram_disp', 'facebook_disp_intl', 'affiliates', 'intl_css_plas'
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
CREATE or replace TABLE `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability`
 AS (SELECT
     a.order_date,
     a.visit_date,
     a.top_channel,
     a.second_channel,
     a.third_channel,
     a.utm_campaign,
     a.utm_medium, 
     coalesce(c.utm_custom2,'') as utm_custom2,
     a.category,
     a.canonical_region,
     a.mapped_region,
     a.device,
     a.flag,
     coalesce(t.seller_tier, 'unknown') AS seller_tier,
     coalesce(sum(a.attr_gms), CAST(0 as NUMERIC)) AS attr_gms,
     coalesce(sum(a.attr_receipt), CAST(0 as NUMERIC)) AS attr_receipt,
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
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
  
END 
