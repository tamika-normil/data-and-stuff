-- VALIDATION

/*

DECLARE config_flag_param STRING DEFAULT "chops.elp_review_images_above_text.desktop"; -- Change me!
DECLARE start_date DATE;
DECLARE end_date DATE;

-- Get experiment's start date and end date
SET (start_date, end_date) = (
  SELECT AS STRUCT
    MAX(DATE(boundary_start_ts)) AS start_date,
    MAX(_date) AS end_date,
  FROM
    `etsy-data-warehouse-prod.catapult_unified.experiment`
  WHERE
    experiment_id = config_flag_param
);

*/


/*

create temp table xp_units as (
  with

*/

/*

  -- Get experiment's bucketed units segments
  units_segments AS (
    SELECT
      bucketing_id,
      variant_id,
      MAX(IF(event_id = "buyer_segment", event_value, NULL)) AS buyer_segment,
      MAX(IF(event_id = "top_channel", event_value, NULL)) AS top_channel
    FROM 
      `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
    WHERE 
      _date = end_date
      AND experiment_id = config_flag_param
      AND event_id IN ("buyer_segment", "top_channel")
    GROUP BY
      1, 2
  ),

*/

/*

get_past_experiments as 
      ( select Catapult_URL as report_link, SUBSTR(Catapult_URL, STRPOS(Catapult_URL, 'catapult/') + length('catapult/'), length(Catapult_URL)) as launch_id
        from etsy-data-warehouse-dev.tnormil.lex_past_experiements),
    get_current_experiments as 
      ( select Catapult_or_Looker as report_link, SUBSTR(Catapult_or_Looker, STRPOS(Catapult_or_Looker, 'catapult/') + length('catapult/'), length(Catapult_or_Looker)) as launch_id
        from etsy-data-warehouse-dev.tnormil.lex_experiments_2025),
    all_exp as 
      (select *
      from get_past_experiments 
      where report_link is not null
      union all
      select *
      from get_current_experiments
      where report_link is not null),
get_config as
    (select a.*, b.config_flag
    from all_exp a
    left join `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` b on a.launch_id = cast(b.launch_id as string)),
get_config_dates as
  (SELECT experiment_id,
    MAX(DATE(boundary_start_ts)) AS start_date,
    MAX(_date) AS end_date,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.experiment` bp
    JOIN 
        get_config as gc on bp.experiment_id = gc.config_flag and gc.config_flag is not null
  group by 1)
  -- Get experiment's bucketed units
    SELECT 
      experiment_id,
      bp.bucketing_id,
      bp.variant_id,
      bp.bucketing_ts,
      start_date,
      end_date
    FROM
      `etsy-data-warehouse-prod.catapult_unified.bucketing_period` AS bp
    JOIN 
      get_config_dates as gc using (experiment_id)
    WHERE
      bp._date = gc.end_date
      -- and start_date >= date_sub(current_date, interval 100 day)
      );

create temp table 
  -- Get KHM aggregated events for experiment's bucketed units
  xp_khm_agg_events AS (
    SELECT
      xp.experiment_id,
      xp.bucketing_id,
      xp.variant_id,
      e.event_id,
      e.event_type,
      e.event_value
    FROM
      `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily` AS e
    INNER JOIN
      xp_units AS xp USING (experiment_id, variant_id, bucketing_id)
    WHERE
      e._date BETWEEN start_date AND end_date
      AND e.event_id IN (
        "backend_cart_payment",
        "bounce",
        "backend_add_to_cart", 
        "checkout_start",  
        "engaged_visit",
        "visits",
        "completed_checkouts",
        "page_count",
        "total_winsorized_gms",
        "total_winsorized_order_value",
        "prolist_total_spend",
        "offsite_ads_one_day_attributed_revenue"
        )
      AND e.bucketing_id_type = 1 -- browser_id
  );

create temp table 
  -- Get KHM aggregated events for experiment's bucketed units by unit
  xp_khm_agg_events_by_unit AS (
    SELECT
      experiment_id,
      variant_id,
      bucketing_id,
      SUM(IF(event_id = "backend_cart_payment", event_value, 0)) AS orders,
      SUM(IF(event_id = "bounce", event_value, 0)) AS bounced_visits,
      COUNTIF(event_id = "backend_add_to_cart") AS atc_count,
      COUNTIF(event_id = "checkout_start") AS checkout_start_count,
      SUM(IF(event_id = "engaged_visit", event_value, 0)) AS engaged_visits,
      SUM(IF(event_id = "visits", event_value, 0)) AS visits,
      SUM(IF(event_id = "completed_checkouts", event_value, 0)) AS completed_checkouts,
      SUM(IF(event_id = "page_count", event_value, 0)) AS page_count,
      SUM(IF(event_id = "total_winsorized_gms", event_value, 0)) AS winsorized_gms,
      SUM(IF(event_id = "total_winsorized_order_value", event_value, 0)) AS winsorized_order_value_sum,
      SUM(IF(event_id = "prolist_total_spend", event_value, 0)) AS prolist_total_spend_sum,
      SUM(IF(event_id = "offsite_ads_one_day_attributed_revenue", event_value, 0)) AS offsite_ads_revenue_sum,
    FROM
      xp_khm_agg_events
    GROUP BY
      1,2,3
  );

create temp table 
  -- Get total units by variant
  xp_total_units_by_variant AS (
    SELECT
      variant_id,
      experiment_id,
      COUNT(bucketing_id) AS total_browsers
    FROM
      xp_units
    GROUP BY
      1,2
  );

*/

begin

create temp table prod_catapult_stats as 
-- Key Health Metrics (Winsorized ACBV and AOV)
SELECT
  -- xp.buyer_segment,
  -- xp.top_channel,
  xp.experiment_id,
  xp.variant_id,
  start_date,
  end_date,
  COUNT(xp.bucketing_id) AS browsers,
  -- SAFE_DIVIDE(COUNT(xp.bucketing_id), tu.total_browsers) AS browsers_share,
  SAFE_DIVIDE(COUNTIF(e.orders > 0), COUNT(xp.bucketing_id)) AS conversion_rate,
  SAFE_DIVIDE(COUNTIF(e.bounced_visits > 0), COUNT(xp.bucketing_id)) AS bounce_rate,
  SAFE_DIVIDE(COUNTIF(e.atc_count > 0), COUNT(xp.bucketing_id)) AS pct_with_atc,
  SAFE_DIVIDE(COUNTIF(e.checkout_start_count > 0), COUNT(xp.bucketing_id)) AS pct_with_checkout_start,
  SAFE_DIVIDE(SUM(e.engaged_visits), COUNT(xp.bucketing_id)) AS mean_engaged_visits,
  SAFE_DIVIDE(SUM(e.visits), COUNT(xp.bucketing_id)) AS mean_visits,
  SAFE_DIVIDE(SUM(e.orders), COUNTIF(e.orders > 0)) AS ocb,
  SAFE_DIVIDE(SUM(e.completed_checkouts), COUNT(xp.bucketing_id)) AS orders_per_browser,
  SAFE_DIVIDE(SUM(e.page_count), COUNT(xp.bucketing_id)) AS pages_per_browser,
  SAFE_DIVIDE(SUM(e.winsorized_gms), COUNTIF(e.orders > 0)) AS winsorized_acbv,
  SAFE_DIVIDE(SUM(e.winsorized_order_value_sum), SUM(e.orders)) AS winsorized_aov,
  SAFE_DIVIDE(SUM(e.prolist_total_spend_sum), COUNT(xp.bucketing_id)) AS prolist_total_spend_per_browser,
  SAFE_DIVIDE(SUM(e.offsite_ads_revenue_sum), COUNT(xp.bucketing_id)) AS offsite_ads_revenue_per_browser,
  SUM(e.prolist_total_spend_sum) as prolist_total_spend_sum,
  sum(e.offsite_ads_revenue_sum) as offsite_ads_revenue_sum,
  sum(e.visits) as visits
FROM
  etsy-bigquery-adhoc-prod._script72e030598bca529ce142953ecf502d1acecde14e.xp_units AS xp
/*LEFT JOIN
  xp_total_units_by_variant AS tu ON tu.variant_id = xp.variant_id*/ -- Uncomment when adding breakdowns
LEFT JOIN
  etsy-bigquery-adhoc-prod._script72e030598bca529ce142953ecf502d1acecde14e.xp_khm_agg_events_by_unit AS e USING (bucketing_id, experiment_id,variant_id)
GROUP BY
  1,2,3,4 -- , 2, tu.total_browsers -- Uncomment when adding breakdowns
ORDER BY
  1,2,3,4; -- , 2;

end
