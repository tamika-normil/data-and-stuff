-- owner: euriekim@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: this query pulls together purchase category by affiliate publisher level. 
-- dependencies: etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans
-- dependencies: etsy-data-warehouse-prod.transaction_mart.all_transactions_categories
-- dependencies: etsy-data-warehouse-prod.buyatt_mart.attr_by_browser
-- dependencies: etsy-data-warehouse-prod.buyatt_mart.visits
-- dependencies: etsy-data-warehouse-prod.buyatt_mart.channel_dimensions
-- dependencies: etsy-data-warehouse-prod.buyatt_rollups.multiplier_log
-- dependencies: etsy-data-warehouse-prod.buyatt_mart.latency_adjustments
-- dependencies: etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic
-- dependencies: etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted
-- dependencies: etsy-data-warehouse-prod.marketing.creator_iq_creators
-- dependencies: etsy-data-warehouse-prod.etsy_v2.countries
-- dependencies: etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic

-- this table references code from the below tables, please reference these for future maintanence:
-- https://github.com/etsy/Rollups/blob/master/auto/p2/daily/Buyer-Attribution_Auto/performance_purchase_category_gifting_deep_dive.sql
-- https://github.com/etsy/Rollups/blob/master/auto/p2/daily/affiliates_tiers.sql

--table to get receipts and % gms share by purchase category for each receipt, cleaned up to only receipt id level and % gms shares
CREATE TEMPORARY TABLE receipt_by_category 
PARTITION BY date
AS (
  WITH receipt_by_category AS (
    SELECT
      t.receipt_id,
      tc.new_category AS purchase_category_main,
      tc.second_level_cat_new AS purchase_category_second,
      tc.third_level_cat_new AS purchase_category_third,
      t.date,
      SUM(t.gms_net) AS gms_per,
    FROM `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
    LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions_categories` tc ON t.transaction_id = tc.transaction_id
    WHERE t.transaction_live>0
      AND t.date >= "2018-01-01" --date updated to 2018
      AND market<>"ipp"
    GROUP BY all
  ),sums_by_receipt AS (
    SELECT
      receipt_id,
      date,
      SUM(gms_per) AS gms_per_receipt,
      COUNT(CONCAT(
        COALESCE(receipt_id,0),
        COALESCE(purchase_category_main,""),
        COALESCE(purchase_category_second,""),
        COALESCE(purchase_category_third,""))) AS types_per_receipt
    FROM receipt_by_category
    GROUP BY 1,2
  )
  SELECT
    a.receipt_id,
    a.date,
    purchase_category_main,
    purchase_category_second,
    purchase_category_third,
    CASE WHEN b.gms_per_receipt = 0.0 THEN 1/NULLIF(b.types_per_receipt,0)
      WHEN b.gms_per_receipt > 0.0 THEN gms_per/NULLIF(b.gms_per_receipt,0)
        END AS percent_cat_of_receipt_gms,
  FROM
    receipt_by_category a
  LEFT JOIN sums_by_receipt b USING (receipt_id)
)
;


--joining attribution tables to purchase category tables
CREATE TEMPORARY TABLE attr_by_receipt 
PARTITION BY visit_date
  AS (
  SELECT
    date(timestamp_seconds(b.buy_date)) as buy_date_date,
    b.buy_date, --purch date
    b.o_visit_id,
    b.o_visit_run_date, --attr click/visit dates
    date(timestamp_seconds(o_visit_run_date)) as visit_date,

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,
    a.percent_cat_of_receipt_gms,

    v.top_channel,
    v.second_channel,
    v.third_channel,
    v.utm_campaign,
    v.utm_medium, 
    v.utm_content,
    v.utm_custom2,
    v.marketing_region,

    (SUM(b.external_source_decay_all) OVER (PARTITION BY b.receipt_id,b.o_visit_id,a.purchase_category_main,a.purchase_category_second,a.purchase_category_third))*
      percent_cat_of_receipt_gms AS attr_receipts,
    (SUM(b.external_source_decay_all*b.gms) OVER (PARTITION BY b.receipt_id,b.o_visit_id,a.purchase_category_main,a.purchase_category_second,a.purchase_category_third))*
      percent_cat_of_receipt_gms AS attr_gms,

  FROM `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` b
  JOIN receipt_by_category a 
    ON b.receipt_id = a.receipt_id
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` v 
    ON (v._date = date(timestamp_seconds(b.o_visit_run_date))
    AND v.visit_id = b.o_visit_id AND v._date>="2017-12-01") --visit date updated to 2017
  WHERE 
    b.o_visit_run_date >= unix_date('2017-12-01') * 86400
    and v.second_channel = 'affiliates'
)
;



--table version for attr by click date
CREATE TEMPORARY TABLE attr_by_click 
PARTITION BY _date
  AS (
  SELECT
    --a.o_visit_run_date,
    a.visit_date as _date,
    a.o_visit_id,

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,

    a.top_channel,
    a.second_channel,
    a.third_channel,
    a.utm_campaign,
    a.utm_medium, 
    a.utm_content,
    a.utm_custom2,
    a.marketing_region,

    SUM(attr_receipts) AS attr_receipts,
    SUM(attr_gms) AS attr_gms,

  FROM attr_by_receipt a
  GROUP BY all
)
;



--table version for attr by purch date
CREATE TEMPORARY TABLE attr_by_purch
PARTITION BY buy_date_date
CLUSTER BY o_visit_id
  AS ( 
  SELECT
    --a.buy_date_date,
    buy_date_date,
    a.o_visit_id,

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,

    a.top_channel,
    a.second_channel,
    a.third_channel,
    a.utm_campaign,
    a.utm_medium, 
    a.utm_content,
    a.utm_custom2,
    a.marketing_region,

    SUM(attr_receipts) AS attr_receipts_purch,
    SUM(attr_gms) AS attr_gms_purch,

  FROM attr_by_receipt a
  GROUP BY all
)
;

--create click and purch dimensions table to improve rollup efficiency
CREATE TEMPORARY TABLE purch_dim
PARTITION BY date AS
  WITH keys AS (
    SELECT
      distinct coalesce(top_channel,'') AS top_channel,
      coalesce(second_channel,'') AS second_channel,
      coalesce(third_channel,'') AS third_channel,
      coalesce(utm_campaign,'') AS utm_campaign,
      coalesce(utm_medium,'') AS utm_medium,
      coalesce(utm_content,'') AS utm_content,
      coalesce(utm_custom2,'') AS utm_custom2
      ,buy_date_date, marketing_region
    FROM attr_by_purch
  )
  SELECT
    p.*, 
    cd.reporting_channel_group, 
    cd.engine,
    cd.tactic_high_level,
    cd.tactic_granular,
    cd.audience,
    m.date,
    m.incrementality_multiplier_current,
    m.incrementality_multiplier_finance
  FROM keys p 
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd 
      ON p.top_channel = coalesce(cd.top_channel,'')
      AND p.second_channel  = coalesce(cd.second_channel,'')
      AND p.third_channel  = coalesce(cd.third_channel,'')
      AND p.utm_campaign  = coalesce(cd.utm_campaign,'')
      AND p.utm_medium  = coalesce(cd.utm_medium,'')
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_rollups.multiplier_log` m
      ON m.date = p.buy_date_date 
      AND m.marketing_region = p.marketing_region
      AND m.reporting_channel_group = cd.reporting_channel_group
      AND m.engine = cd.engine
      AND m.tactic_high_level = cd.tactic_high_level
      AND m.tactic_granular = cd.tactic_granular
      AND m.audience = cd.audience
      AND m.date >= '2018-01-01'
      AND cd.reporting_channel_group IS NOT NULL;

CREATE TEMPORARY TABLE click_dim
PARTITION BY date AS
  WITH keys AS (
    SELECT
      distinct coalesce(top_channel,'') AS top_channel,
      coalesce(second_channel,'') AS second_channel,
      coalesce(third_channel,'') AS third_channel,
      coalesce(utm_campaign,'') AS utm_campaign,
      coalesce(utm_medium,'') AS utm_medium,
      coalesce(utm_content,'') AS utm_content,
      coalesce(utm_custom2,'') AS utm_custom2
      ,_date, marketing_region
    FROM attr_by_click
    WHERE _date >= '2017-12-01')
  SELECT p.*, 
    cd.reporting_channel_group, 
    cd.engine,
    cd.tactic_high_level,
    cd.tactic_granular,
    cd.audience,
    m.date,
    m.incrementality_multiplier_current,
    m.incrementality_multiplier_finance
  FROM keys p 
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd 
    ON p.top_channel = coalesce(cd.top_channel,'')
    AND p.second_channel  = coalesce(cd.second_channel,'')
    AND p.third_channel  = coalesce(cd.third_channel,'')
    AND p.utm_campaign  = coalesce(cd.utm_campaign,'')
    AND p.utm_medium  = coalesce(cd.utm_medium,'')
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_rollups.multiplier_log` m
    ON m.date = p._date 
    AND m.marketing_region = p.marketing_region
    AND m.reporting_channel_group = cd.reporting_channel_group
    AND m.engine = cd.engine
    AND m.tactic_high_level = cd.tactic_high_level
    AND m.tactic_granular = cd.tactic_granular
    AND m.audience = cd.audience
    AND m.date >= '2017-12-01'
    AND cd.reporting_channel_group IS NOT NULL;


CREATE TEMPORARY TABLE attr_visits_union
PARTITION BY date
CLUSTER BY
  purchase_category_main,
  purchase_category_second,
  purchase_category_third
  AS 
  SELECT
    a._date AS date,

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,

    a.top_channel,
    a.second_channel,
    a.third_channel,
    a.utm_campaign,
    a.utm_medium, 
    a.utm_content,
    a.utm_custom2,
    a.marketing_region,

    cd.incrementality_multiplier_current,
    cd.incrementality_multiplier_finance,
    cd.reporting_channel_group,

    a.attr_receipts,
    a.attr_gms,

    CAST(NULL AS FLOAT64) AS attr_receipts_purch,
    CAST(NULL AS FLOAT64) AS attr_gms_purch,

  FROM attr_by_click a
  LEFT JOIN click_dim cd 
    ON coalesce(a.top_channel,'') = cd.top_channel
    AND coalesce(a.second_channel,'')  = cd.second_channel
    AND coalesce(a.third_channel,'')  = cd.third_channel
    AND coalesce(a.utm_campaign,'')  = cd.utm_campaign
    AND coalesce(a.utm_medium ,'') = cd.utm_medium
    AND coalesce(a.utm_content ,'') = cd.utm_content
    AND coalesce(a.utm_custom2,'') = cd.utm_custom2
    AND a._date = cd.date
    AND a.marketing_region = cd.marketing_region
  WHERE a._date>='2017-12-01'
;

INSERT INTO attr_visits_union
--second half of union to pull data from and format attr by purch
  (SELECT
    p.buy_date_date AS date,

    p.purchase_category_main,
    p.purchase_category_second,
    p.purchase_category_third,

    p.top_channel,
    p.second_channel,
    p.third_channel,
    p.utm_campaign,
    p.utm_medium, 
    p.utm_content,
    p.utm_custom2,
    p.marketing_region,

    cd.incrementality_multiplier_current,
    cd.incrementality_multiplier_finance,
    cd.reporting_channel_group,

    NULL AS attr_receipts,
    NULL AS attr_gms,

    p.attr_receipts_purch,
    p.attr_gms_purch,

  FROM attr_by_purch p
  LEFT JOIN purch_dim cd 
    ON coalesce(p.top_channel,'') = cd.top_channel
    AND coalesce(p.second_channel,'')  = cd.second_channel
    AND coalesce(p.third_channel,'')  = cd.third_channel
    AND coalesce(p.utm_campaign,'')  = cd.utm_campaign
    AND coalesce(p.utm_medium ,'') = cd.utm_medium
    AND coalesce(p.utm_content ,'') = cd.utm_content
    AND coalesce(p.utm_custom2 ,'') = cd.utm_custom2
    AND p.buy_date_date = cd.date
    AND p.marketing_region = cd.marketing_region
);


--sums up data from previous union
CREATE TEMPORARY TABLE attr_visits_union_sum 
  AS ( 
  SELECT 
  date,

  purchase_category_main,
  purchase_category_second,
  purchase_category_third,

  third_channel,
  second_channel, 
  top_channel,
  utm_campaign,
  utm_medium,
  utm_content,
  utm_custom2,
  marketing_region,

  COALESCE(incrementality_multiplier_current,1.0) AS incrementality_multiplier_current,
  COALESCE(incrementality_multiplier_finance,1.0) AS incrementality_multiplier_finance,
  reporting_channel_group,

  SUM(COALESCE(attr_receipts,0.0)) AS attr_receipts,
  SUM(COALESCE(attr_gms,0.0)) AS attr_gms,

  SUM(COALESCE(attr_receipts_purch,0.0)) AS attr_receipts_purch,
  SUM(COALESCE(attr_gms_purch,0.0)) AS attr_gms_purch,

  FROM attr_visits_union
  GROUP BY all
);

--creating metrics ready for adjustments and multipliers
CREATE TEMPORARY TABLE attr_visits 
PARTITION BY date
AS ( 

  SELECT 
    a.date,

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,

    a.third_channel,
    a.second_channel, 
    a.top_channel,
    a.utm_campaign,
    a.utm_medium,
    a.utm_content,
    a.utm_custom2,
    a.marketing_region,

    a.reporting_channel_group,

    a.incrementality_multiplier_current,
    a.incrementality_multiplier_finance,

    FALSE AS with_latency,

    a.attr_gms,
    a.attr_receipts,


    CAST(a.attr_gms AS FLOAT64) AS attr_gms_adj,
    CAST(a.attr_receipts AS FLOAT64) AS attr_receipts_adj,

    --metrics with current multiplier
    COALESCE(a.attr_gms,0)*a.incrementality_multiplier_current AS attr_gms_mult,
    COALESCE(a.attr_receipts,0.0)*a.incrementality_multiplier_current AS attr_receipts_mult,

    --purchase date metrics with current multiplier
    COALESCE(a.attr_gms_purch,0)*a.incrementality_multiplier_current AS attr_gms_mult_purch,
    COALESCE(a.attr_receipts_purch,0.0)*a.incrementality_multiplier_current AS attr_receipts_mult_purch,    

    --adjusted metrics with current multiplier (not yet adjusted; currently just click date data)
    CAST(COALESCE(a.attr_gms,0.0)*a.incrementality_multiplier_current AS FLOAT64) AS attr_gms_adj_mult,
    CAST(COALESCE(a.attr_receipts,0.0)*a.incrementality_multiplier_current AS FLOAT64) AS attr_receipts_adj_mult,

    --metrics with finance multiplier
    COALESCE(a.attr_gms,0.0)*a.incrementality_multiplier_finance AS attr_gms_mult_fin,
    COALESCE(a.attr_receipts,0.0)*a.incrementality_multiplier_finance AS attr_receipts_mult_fin,

    --purchase date metrics with finance multiplier
    COALESCE(a.attr_gms_purch,0.0)*a.incrementality_multiplier_finance AS attr_gms_mult_fin_purch,
    COALESCE(a.attr_receipts_purch,0.0)*a.incrementality_multiplier_finance AS attr_receipts_mult_fin_purch,

    --adjusted metrics with finance multiplier (not yet adjusted; currently just click date data)
    CAST(COALESCE(a.attr_gms,0.0)*a.incrementality_multiplier_finance AS FLOAT64) AS attr_gms_adj_mult_fin,
    CAST(COALESCE(a.attr_receipts,0.0)*a.incrementality_multiplier_finance AS FLOAT64) AS attr_receipts_adj_mult_fin,

    --purchase date metrics
    a.attr_gms_purch,
    a.attr_receipts_purch,

  FROM attr_visits_union_sum a
)
;

--table joining with latency adjustments for past 30 days and using purchase/mature click date data for dates prior
CREATE TEMPORARY TABLE purch_cat_aff
  AS (
--first query for most recent 30 days
  (SELECT
    a.date,

    a.third_channel,
    a.second_channel, 
    a.top_channel,
    a.utm_campaign,
    a.utm_medium,
    a.utm_content,
    a.utm_custom2,
    a.marketing_region,

	utm_content as publisher_id,
    case when utm_content =  '946733' then REGEXP_replace( utm_custom2 , r'(_p|_p_tiktok|_p_facebook|_tiktok|_meta)$','') else '0' end as subnetwork_id, 

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,

    a.reporting_channel_group,

    a.incrementality_multiplier_current,
    a.incrementality_multiplier_finance,

    TRUE AS with_latency,

    a.attr_gms,
    a.attr_receipts,

    a.attr_gms_purch,
    a.attr_receipts_purch,

    --adjusted metrics (latency included metrics. For non-paid channels, where we don't have a latency prediction, this is purchase date data.)
    CASE
      WHEN l.adjustment_factor_gms IS NOT NULL THEN a.attr_gms_purch*l.adjustment_factor_gms 
        ELSE attr_gms_purch END
          AS attr_gms_adj,
    CASE
      WHEN l.adjustment_factor IS NOT NULL THEN a.attr_receipts_purch*l.adjustment_factor 
        ELSE a.attr_receipts_purch END
          AS attr_receipts_adj,

    --metrics with current multiplier
    attr_gms_mult,
    attr_receipts_mult,

    --adjusted metrics with current multiplier
    --COALESCE TO DEFAULT TO PURCH DATE IF NULL
    COALESCE(
      (CASE
        WHEN l.adjustment_factor_gms IS NOT NULL
          THEN a.attr_gms_purch*l.adjustment_factor_gms
        ELSE a.attr_gms_purch END)
      ,0)
      *COALESCE(a.incrementality_multiplier_current,1) AS attr_gms_adj_mult,
    COALESCE(
      (CASE
        WHEN l.adjustment_factor IS NOT NULL
          THEN a.attr_receipts_purch*l.adjustment_factor
        ELSE a.attr_receipts_purch END)
      ,0)
      *COALESCE(a.incrementality_multiplier_current,1) AS attr_receipts_adj_mult,

    --metrics with finance multiplier
    attr_gms_mult_fin,
    attr_receipts_mult_fin,

    --adjusted metrics with finance multiplier
    --COALESCE TO DEFAULT TO PURCH DATE IF NULL
    COALESCE(
      (CASE
        WHEN l.adjustment_factor_gms IS NOT NULL
          THEN a.attr_gms_purch*l.adjustment_factor_gms
        ELSE a.attr_gms_purch END)
      ,0)
      *COALESCE(a.incrementality_multiplier_finance,1) AS attr_gms_adj_mult_fin,
    COALESCE(
      (CASE
        WHEN l.adjustment_factor IS NOT NULL
          THEN a.attr_receipts_purch*l.adjustment_factor
        ELSE a.attr_receipts_purch END)
      ,0)
      *COALESCE(a.incrementality_multiplier_finance,1) AS attr_receipts_adj_mult_fin,

  FROM attr_visits a
    LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.latency_adjustments` l
      ON if(a.reporting_channel_group = 'Social Creator Co','Affiliates',a.reporting_channel_group)  = l.reporting_channel_group
        AND l.marketing_region = CASE WHEN a.marketing_region in ('US', 'GB', 'DE') 
                                  THEN a.marketing_region ELSE 'ROW' END
        AND l.date = a.date
  WHERE a.date > DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY))
UNION all 
--second query for data prior to 30 days
  (SELECT
    a.date,

    a.third_channel,
    a.second_channel, 
    a.top_channel,
    a.utm_campaign,
    a.utm_medium,
    a.utm_content,
    a.utm_custom2,
    a.marketing_region,

    utm_content as publisher_id,
    case when utm_content =  '946733' then REGEXP_replace( utm_custom2 , r'(_p|_p_tiktok|_p_facebook|_tiktok|_meta)$','') else '0' end as subnetwork_id, 

    a.purchase_category_main,
    a.purchase_category_second,
    a.purchase_category_third,

    a.reporting_channel_group,

    a.incrementality_multiplier_current,
    a.incrementality_multiplier_finance,

    a.with_latency,

    a.attr_gms,
    a.attr_receipts,

    a.attr_gms_purch,
    a.attr_receipts_purch,

    --adjusted metrics (purchase date for non-paid channels, click date mature for paid channels)
    CASE
      WHEN top_channel NOT IN ('us_paid','intl_paid') THEN attr_gms_purch
      ELSE attr_gms_adj END
        AS attr_gms_adj,
    CASE
      WHEN top_channel NOT IN ('us_paid','intl_paid') THEN attr_receipts_purch
      ELSE attr_receipts_adj END
        AS attr_receipts_adj,

    --metrics with current multiplier
    attr_gms_mult,
    attr_receipts_mult,

    --adjusted metrics with multiplier (purchase date for non-paid channels, click date mature for paid channels)
    COALESCE(
      CASE
        WHEN top_channel NOT IN ('us_paid','intl_paid') THEN attr_gms_mult_purch
        ELSE attr_gms_adj_mult END
      ,0)
        AS attr_gms_adj_mult,
    COALESCE(
      CASE
        WHEN top_channel NOT IN ('us_paid','intl_paid') THEN attr_receipts_mult_purch
        ELSE attr_receipts_adj_mult END
      ,0)
        AS attr_receipts_adj_mult,

    --metrics with finance multiplier
    attr_gms_mult_fin,
    attr_receipts_mult_fin,

    --adjusted metrics with finance multiplier (purchase date for non-paid channels, click date mature for paid channels)
    COALESCE(
      CASE
        WHEN top_channel NOT IN ('us_paid', 'intl_paid') THEN attr_gms_mult_fin_purch
        ELSE attr_gms_adj_mult_fin END
      ,0)
        AS attr_gms_adj_mult_fin,
    COALESCE(
      CASE
        WHEN top_channel NOT IN ('us_paid', 'intl_paid') THEN attr_receipts_mult_fin_purch
        ELSE attr_receipts_adj_mult_fin END
      ,0)
        AS attr_receipts_adj_mult_fin,

  FROM attr_visits a
  WHERE date <= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY))
)
;

CREATE TEMPORARY TABLE purch_cat_aff_weekly as (
select date_trunc( date , week(monday)) as date,
publisher_id,
subnetwork_id,

    purchase_category_main,
    purchase_category_second,
    purchase_category_third,

    reporting_channel_group,

	sum(attr_gms) as attr_gms,
	sum(attr_receipts) as attr_receipts,

	sum(attr_gms_adj) as attr_gms_adj,
	sum(attr_receipts_adj) as attr_receipts_adj,

	sum(attr_gms_adj_mult) as attr_gms_adj_mult,
	sum(attr_receipts_adj_mult) as attr_receipts_adj_mult,

	sum(attr_gms_adj_mult_fin) as attr_gms_adj_mult_fin,
	sum(attr_receipts_adj_mult_fin) as attr_receipts_adj_mult_fin,

	from purch_cat_aff
	group by all

)
;


CREATE TEMPORARY TABLE cc as 
    (with cc_direct_base as 
	    (select '946733' as publisher_id, cast(PublisherId as string) as subnetwork_id, CASE
	    WHEN iso_country_code = 'AU' THEN 'AU'
	    WHEN iso_country_code = 'CA' THEN 'CA'
	    WHEN iso_country_code = 'DE' THEN 'DE'
	    WHEN iso_country_code = 'ES' THEN 'ES'
	    WHEN iso_country_code = 'EU' THEN 'EU'
	    WHEN iso_country_code = 'FR' THEN 'FR'
	    WHEN iso_country_code = 'GB' THEN 'GB'
	    WHEN iso_country_code = 'IT' THEN 'IT'
	    WHEN iso_country_code = 'NL' THEN 'NL'
	    WHEN iso_country_code = 'SC' THEN 'SC'
	    WHEN iso_country_code = 'US' THEN 'US'
	    ELSE 'ROW'
	    END AS country,
	    PublisherName,
	    FlagshipSocialNetwork,
	    Category,
	    date_trunc(coalesce( PARSE_DATE('%Y-%m-%d', DateJoinedPortal) , PARSE_DATE('%Y-%m-%d', DatePublisherStateChanged)),week(monday)) as date,
	    row_number() over (partition by publisherid order by PartitionDate desc) as rnk
	    from etsy-data-warehouse-prod.marketing.creator_iq_creators a
	    join etsy-data-warehouse-prod.etsy_v2.countries c on a.country = c.name
	    where StatusCategory = 'In Network'
	    qualify rnk = 1)
	select a.publisher_id, a.subnetwork_id, a.date as date_joined, FlagshipSocialNetwork, Category, publishername,
    country
    from cc_direct_base a
)
;

 CREATE TEMPORARY TABLE aff as 
    with aff_base as 
	    (select publisher_id, date_trunc( date_joined , week(monday)) as date,publisher
	    from  `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic`
	    where publisher_id <> '946733'),
	aff_perf as (
		select date_trunc(channel_overview_restricted.date, week(monday)) as date, utm_content as publisher_id,
    	case when utm_content =  '946733' then REGEXP_replace( utm_custom2 , r'(_p|_p_tiktok|_p_facebook|_tiktok|_meta)$','') else '0' end as subnetwork_id, 
    	SUM( coalesce(channel_overview_restricted.visits, 0)  )  as visits, 
 		FROM `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted`  AS channel_overview_restricted
    	where second_level_channel = 'affiliates'
    	group by 1,2,3)
	select ab.publisher_id, '0' as subnetwork_id, publisher, ab.date as date_joined, min(case when visits > 0 then a.date end) as first_click_date, max(case when visits > 0 then a.date end) as last_click_date
    from aff_base ab 
    left join aff_perf a on ab.publisher_id = a.publisher_id
    group by 1,2,3,4,5  
 ;

create or replace table `etsy-data-warehouse-prod.rollups.top_category_by_affliates` 
	as (
	select 
	p.date,
	p.publisher_id,
	p.subnetwork_id,
	c.date_joined,
	c.FlagshipSocialNetwork, 
	c.Category,
	c.publishername, 
	c.country,


    p.purchase_category_main,
    p.purchase_category_second,
    p.purchase_category_third,

    p.reporting_channel_group,
    att.tactic,

	--sum(attr_gms) as attr_gms,
	--sum(attr_receipts) as attr_receipts,

	--sum(attr_gms_adj) as attr_gms_adj,
	--sum(attr_receipts_adj) as attr_receipts_adj,

	--sum(attr_gms_adj_mult) as attr_gms_adj_mult,
	--sum(attr_receipts_adj_mult) as attr_receipts_adj_mult,

	sum(attr_gms_adj_mult_fin) as attr_gms_adj_mult_fin,
	sum(attr_receipts_adj_mult_fin) as attr_receipts_adj_mult_fin,
from purch_cat_aff_weekly p
join (select distinct publisher_id, subnetwork_id, date_joined as date_joined, FlagshipSocialNetwork, Category,publishername, country from cc 
	union all
    select distinct publisher_id, subnetwork_id, date_joined as date_joined, string(null), string(null), publisher, string(null) from aff
    ) c using (publisher_id, subnetwork_id)
left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic att on c.publisher_id = att.publisher_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
;
