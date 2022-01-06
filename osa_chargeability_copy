BEGIN 

CREATE TEMP TABLE attr_visits
  AS SELECT
      v.visit_id,
      v.receipt_id,
      v.visit_date,
      v.order_date,
      v.top_channel,
      v.second_channel,
      v.utm_campaign,
      v.category,
      v.channel_int,
      v.canonical_region,
      v.mapped_region,
      v.attr_gms,
      v.gms,
      v.attr_receipt,
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
              WHEN b.second_channel = 'pinterest_disp' THEN 5
	            WHEN b.second_channel = 'affiliates' and (b.third_channel = 'affiliates_feed' or b.third_channel = 'affiliates_widget' ) THEN 6		
              WHEN lower(b.utm_campaign) like 'gdn_%' then 7
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
            --  attr and total receipt GMS for the chargeability calcs
            CAST(a.external_source_decay_all * a.gms as NUMERIC) AS attr_gms,
            a.gms,
            a.external_source_decay_all as attr_receipt,
            coalesce(CAST(regexp_extract(b.landing_event_url, '(\?i)listing\\/(\\d{1,9})', 1, 1) as INT64), -1) AS landing_listing_id
         FROM
           `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` as a
         INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` as b
         ON a.o_visit_run_date = b.run_date and a.o_visit_id = b.visit_id
         WHERE a.receipt_timestamp >= TIMESTAMP '2020-02-04 00:00:00' -- when we began writing USD acquisition fees reliably
          AND a.receipt_timestamp < CAST(CAST(current_date() as DATETIME) AS TIMESTAMP)
          AND a.o_visit_run_date >= UNIX_SECONDS(CAST(CAST(DATE '2020-01-04' as DATETIME) AS TIMESTAMP))
                  AND a.o_visit_run_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
                  AND b._date >= '2020-01-04'
                  AND b._date < current_date()
          AND (b.second_channel IN(
                     'gpla', 'intl_gpla', 'facebook_disp', 'bing_plas', 'intl_bing_plas', 'pinterest_disp', 'instagram_disp', 'facebook_disp_intl', 'affiliates'
                   ) or b.third_channel in ('google_gdn','other_intl_ppc'))
          AND upper(b.utm_campaign) NOT LIKE '%_CUR_%'
       ORDER BY
         --  remove curated from this
         1 NULLS LAST,
         2
     ) AS v;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.offsite_ads_chargeability`
 AS SELECT
     a.order_date,
     a.visit_date,
     a.top_channel,
     a.second_channel,
     a.utm_campaign,
     a.category,
     a.canonical_region,
     a.mapped_region,
     a.device,
     coalesce(sum(a.attr_gms), CAST(0 as NUMERIC)) AS attr_gms,
     coalesce(sum(CASE
       WHEN r.receipt_id IS NOT NULL
        AND a.order_channel_rank = 1 THEN a.gms
     END), CAST(0 as NUMERIC)) AS chargeable_gms,
     coalesce(sum(CASE
       WHEN r.receipt_id IS NOT NULL
        AND a.order_channel_rank = 1 THEN r.acquisition_fee_usd / 100
     END), CAST(0 as NUMERIC)) AS advertising_revenue,
      coalesce(sum(CASE
       WHEN r1.receipt_id IS NOT NULL
        THEN (r1.acquisition_fee_usd / 100) * attr_receipt
     END), CAST(0 as NUMERIC)) AS mta_advertising_revenue    
   FROM
     attr_visits AS a
     LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r ON a.receipt_id = r.receipt_id
      AND a.channel_int = r.channel
    LEFT OUTER JOIN `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` AS r1 ON a.receipt_id = r1.receipt_id  
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;    

END
