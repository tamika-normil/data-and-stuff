-- owner: vbhuta@etsy.com
-- owner_team: marketinganalytics@etsy.com
-- description: This scripts aggregates internal performance and external spend data at a campaign level.
-- dependencies: etsy-data-warehouse-prod.buyatt_rollups.channel_overview
-- dependencies: etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date
-- dependencies: etsy-data-warehouse-prod.marketing.awin_spend_data
-- dependencies: etsy-data-warehouse-prod.marketing.bing_campaign_performance
-- dependencies: etsy-data-warehouse-prod.marketing.fb_spend_daily
-- dependencies: etsy-data-warehouse-prod.marketing.adwords_campaign_performance_report

#The performance marketing team began leveraging utm_custom2 in March 2020. So, we cannot link spend and performance data on a campaign level prior to then.
#This scripts aggregates internal performance and external spend data at a campaign level. 
#At the end of the script, `etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker_historical`, the data source for historical account level pre March 2020, is merged with current campaign level data.

BEGIN

-- collect cost, clicks, and impression data from external marketing data sources
CREATE TEMPORARY TABLE all_markt as 
     (WITH adwords AS (
    SELECT
        day,
        --  ,split_part(regexp_replace(account, '[^\w]','_'), '_', 1)||' '||split_part(regexp_replace(account, '[^\w]','_'), '_', 2)||' '||split_part(regexp_replace(account, '[^\w]','_'), '_', 3)
        CASE
          WHEN upper(account) LIKE '%SHOWCASE%' THEN CASE
            WHEN upper(account) LIKE '%\\_UK%' THEN 'PLA UK Showcase - google'
            WHEN upper(account) LIKE '%\\_DE%' THEN 'PLA DE Showcase - google'
            WHEN upper(account) LIKE '%\\_CA%' THEN 'PLA CA Showcase - google'
            WHEN upper(account) LIKE '%\\_FR%' THEN 'PLA FR Showcase - google'
            WHEN upper(account) LIKE '%\\_AU%' THEN 'PLA AU Showcase - google'
            ELSE 'PLA US Showcase - google'
          END
          WHEN upper(account) like '% VIDEO%' then concat('YouTube ',coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(2)],'other'),' - google')
          WHEN upper(account) LIKE 'ETSY SHOPPING%' THEN 'PLA US - google'
          WHEN upper(account) LIKE 'ETSY DISPLAY%' THEN concat('Native Display ',coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],'Other'),' - google')
          WHEN upper(concat(coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(2)],''), ' ', coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],''))) LIKE 'DSA ' THEN 'SEM DSA US - google'
          WHEN upper(account) LIKE 'ETSY SEM US%BRANDED%' THEN 'SEM Brand US - google'
          WHEN upper(account) LIKE 'ETSY SEM US%NB%' THEN 'SEM non-Brand US - google'
          WHEN upper(upper(split(campaign_name, '_')[SAFE_ORDINAL(3)])) = 'DSA' then concat('SEM ', upper(split(campaign_name, '_')[SAFE_ORDINAL(2)]), ' DSA - google')
          WHEN length(coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(6)],'')) > 0 THEN concat('SEM', ' ', coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],''), ' ', coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(6)],''), ' - google')
          ELSE concat(coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(2)],''), ' ', coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],''), ' - google')
        END AS account_name,
        campaign_id,
        lower(campaign_name) AS campaign_name,
        sum(clicks) AS clicks,
        sum(cost) AS cost,
        sum(impressions) AS impressions,
        'google' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.adwords_campaign_performance_report`
      GROUP BY 1, 2, 3, 4
      ORDER BY 1 DESC NULLS FIRST, 3 DESC NULLS FIRST
  ), bing AS (
    SELECT
        gregorian_date AS day,
        --  ,split_part(regexp_replace(account_name, '[^\w]','_'), '_', 1)||' '||split_part(regexp_replace(account_name, '[^\w]','_'), '_', 2)||' '||split_part(regexp_replace(account_name, '[^\w]','_'), '_', 3)
        CASE
          WHEN upper(account_name) LIKE '%SHOPPING%' THEN CASE
            WHEN upper(account_name) LIKE '%UK%' THEN 'PLA UK - bing'
            WHEN upper(account_name) LIKE '%DE%' THEN 'PLA DE - bing'
            WHEN upper(account_name) LIKE '%CA%' THEN 'PLA CA - bing'
            WHEN upper(account_name) LIKE '%FR%' THEN 'PLA FR - bing'
            WHEN upper(account_name) LIKE '%AU%' THEN 'PLA AU - bing'
            ELSE 'PLA US - bing'
          END
          WHEN upper(account_name) LIKE '%MSAN' THEN 
            CASE
            WHEN upper(account_name) LIKE '%UK%' THEN 'Native Display UK - bing'
            WHEN upper(account_name) LIKE '%DE%' THEN 'Native Display DE - bing'
            WHEN upper(account_name) LIKE '%CA%' THEN 'Native Display CA - bing'
            WHEN upper(account_name) LIKE '%FR%' THEN 'Native Display FR - bing'
            WHEN upper(account_name) LIKE '%AU%' THEN 'Native Display AU - bing'
            ELSE 'Native Display US - bing'
          END
          WHEN upper(concat(coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(2)],''), ' ', coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],''))) LIKE '%DSA%' THEN 'SEM DSA US - bing'
          WHEN upper(account_name) LIKE '%US%BRANDED%' THEN 'SEM Brand US - bing'
          WHEN upper(account_name) LIKE '%US%NB%' THEN 'SEM non-Brand US - bing'
          when length(split(replace(account_name,'"',''),' ')[SAFE_ORDINAL(5)])>0 then concat('SEM ',split(replace(account_name,'"',''),' ')[SAFE_ORDINAL(3)],' ',split(replace(account_name,'"',''),' ')[SAFE_ORDINAL(5)],' - bing')
          -- WHEN length(coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(7)],'')) > 0 THEN concat('SEM', ' ', coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(4)],''), ' ', coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(7)],''), ' - bing')
          ELSE concat(coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],''), ' ', coalesce(split(regexp_replace(account_name, '[^\\w]', '_'), '_')[SAFE_ORDINAL(4)],''), ' - bing')
        END AS account_name,
        campaign_id,
        lower(campaign_name) AS campaign_name,
        sum(clicks) AS clicks,
        sum(spend) AS cost,
        sum(impressions) AS impressions,
        'bing' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.bing_campaign_performance`
      GROUP BY 1, 2, 3, 4
      ORDER BY 1 DESC NULLS FIRST, 3 DESC NULLS FIRST
  ), facebook AS (
    SELECT
        CAST(date_start as DATE) AS day,
        CASE
          WHEN account_name = '"Buyer Acquisition"' THEN CASE
            WHEN upper(campaign_name) LIKE '%DABA%'
             OR upper(campaign_name) LIKE '%DPA%'
             OR upper(campaign_name) LIKE '%DYNAMIC%' THEN 'Facebook - Dynamic'
            WHEN upper(campaign_name) LIKE '%CURATED%' THEN 'Facebook - Curated'
            when upper(campaign_name) LIKE '% ASC %' THEN 'Facebook - Optimized'
            WHEN upper(account_name) LIKE '%BUYER ACQUISITION%' THEN 'Facebook - Other'
            ELSE CAST(NULL as STRING)
          END
          WHEN account_name like '%Global%' and (campaign_name like '%Video%' or campaign_name like '%PSV%') then 
            case when campaign_name like '%20_%' and campaign_name not like '%2021%' 
              then concat('Facebook Video - ',coalesce(trim(split(split(campaign_name,'|')[SAFE_ORDINAL(5)],'"')[SAFE_ORDINAL(1)]),'Other'))
            else concat('Facebook Video - ',coalesce(trim(split(split(campaign_name,'|')[SAFE_ORDINAL(1)],'"')[SAFE_ORDINAL(1)]),'Other')) 
            end 
          ELSE CASE
            WHEN upper(campaign_name) LIKE '%DABA%'
             OR upper(campaign_name) LIKE '%DPA%'
             OR upper(campaign_name) LIKE '%DYNAMIC%' THEN concat('Facebook', coalesce(split(coalesce(split(account_name, '-')[SAFE_ORDINAL(2)],''), '"')[SAFE_ORDINAL(1)],''), ' - Dynamic')
            WHEN upper(campaign_name) LIKE '%CURATED%' THEN concat('Facebook', coalesce(split(coalesce(split(account_name, '-')[SAFE_ORDINAL(2)],''), '"')[SAFE_ORDINAL(1)],''), ' - Curated')
            WHEN upper(campaign_name) LIKE '% ASC %' THEN concat('Facebook', coalesce(split(coalesce(split(account_name, '-')[SAFE_ORDINAL(2)],''), '"')[SAFE_ORDINAL(1)],''), ' - Optimized')
            WHEN upper(account_name) LIKE '%BUYER ACQUISITION%' THEN concat('Facebook', coalesce(split(coalesce(split(account_name, '-')[SAFE_ORDINAL(2)],''), '"')[SAFE_ORDINAL(1)],''), ' - Other')
            ELSE CAST(NULL as STRING)
          END
        END AS account_name,
        campaign_id,
        lower(campaign_name) AS campaign_name,
        sum(outbound_clicks) AS clicks,
        sum(coalesce(spend_usd, spend)) AS cost,
        --  using spend_usd, which has international spend in USD
        sum(impressions) AS impressions,
        'facebook' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.fb_spend_daily`
      GROUP BY 1, 2, 3, 4
      ORDER BY 1 DESC NULLS FIRST, 3 DESC NULLS FIRST
  ), exchange AS (
    SELECT
        source_currency,
        source_precision,
        target_currency,
        target_precision,
        market_rate,
        seller_rate,
        buyer_rate,
        create_date,
        date,
        creation_tsz,
        lead(create_date, 1) OVER (PARTITION BY source_currency, target_currency ORDER BY create_date) - 1 AS cw_thru_date
      FROM
        `etsy-data-warehouse-prod.materialized.exchange_rates`),
  affiliate AS (
    SELECT
        DATE(transaction_date) AS day,
        concat(substr(CAST(publisher_id as STRING), 1, 80), ' - ', region) AS account_name,
        sum(a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS cost,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales,
        0 AS impressions,
        'affiliate' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` AS a
        LEFT OUTER JOIN exchange AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) )
        WHERE commission_status in ('pending','approved')
      GROUP BY 1, 2 ),
 pinterest as (
       SELECT a.date AS day,
       case when MARKET='US' then 'Pinterest - US'
          when MARKET='GB' then 'Pinterest - UK'
          else concat('Pinterest - ',MARKET)
       end as account_name,
       CAMPAIGN_ID,
       CAMPAIGN_NAME,
       sum(SPEND_IN_DOLLAR * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) as cost,
       sum(PIN_CLICK_PAID) as clicks,
       sum(IMPRESSION_PAID) as impressions,
       'pinterest' as engine
       FROM 
       -- source currency for CA is USD; other nondomestic markets is EUR per CCA data team
       (select *, case when market in ('US','CA') then 'USD' else 'EUR' end as source_currency from `etsy-data-warehouse-prod.marketing.pinterest_spend_daily`) a
         LEFT OUTER JOIN exchange AS b_0 ON a.source_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(a.date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(a.date AS TIMESTAMP)) )
       where AD_GROUP_ID IS NULL
       group by 1, 2, 3, 4
),   
 all_markt AS (
    SELECT
        --  for newer campaigns you only need utm_medium check, as we launched display_product/display_curated in 2019
        --  for newer campaigns you only need utm_medium check, as we launched display_product/display_curated in 2019
        adwords.day,
        adwords.account_name,
        adwords.campaign_id,
        adwords.campaign_name,
        adwords.clicks,
        adwords.cost,
        adwords.impressions,
        adwords.engine
      FROM
        adwords
    UNION ALL
    SELECT
        bing.day,
        bing.account_name,
        bing.campaign_id,
        bing.campaign_name,
        bing.clicks,
        bing.cost,
        bing.impressions,
        bing.engine
      FROM
        bing
    UNION ALL
    SELECT
        facebook.day,
        facebook.account_name,
        facebook.campaign_id,
        facebook.campaign_name,
        facebook.clicks,
        facebook.cost,
        facebook.impressions,
        facebook.engine
      FROM
        facebook
    UNION ALL
    SELECT
        pinterest.day,
        pinterest.account_name,
        pinterest.campaign_id,
        pinterest.campaign_name,
        pinterest.clicks,
        pinterest.cost,
        pinterest.impressions,
        pinterest.engine
      FROM
        pinterest
    UNION ALL
    SELECT
        affiliate.day,
        affiliate.account_name,
        null as campaign_id,
        null as campaign_name,
        0 as clicks,
        affiliate.cost,
        0 as impressions,
        affiliate.engine
      FROM
        affiliate    
      )
   select * from all_markt);                                                                                              

-- use a window function to get the latest campaign name for each combination of account name and campaign id. several campaign ids have more than one campaign name. this is why we need the window function to choose the name associated with the most recent date.                            
CREATE TEMPORARY TABLE all_markt_campaign_name as 
     (SELECT DISTINCT account_name, campaign_id, campaign_name, ROW_NUMBER() OVER ( PARTITION BY account_name, campaign_id ORDER BY DAY DESC ) AS rank
     from  all_markt
     qualify rank = 1);

-- channel overview with account name created to merge the internal and external data sources                                                           
CREATE TEMPORARY TABLE internal as 
     (SELECT
        date,
        b.channel,
        case WHEN upper(a.third_channel) LIKE '%AFFILIATE%' THEN "" else a.utm_custom2 end as utm_custom2,
        CASE
          when engine='google' and a.utm_medium not like '%video%' then 'cpc'
          when engine='bing' and a.utm_medium not like '%video%' then 'cpc'
          WHEN upper(a.third_channel) LIKE '%AFFILIATE%' THEN ""
          ELSE a.utm_medium
        END AS utm_medium,
        CASE WHEN upper(a.third_channel) LIKE '%AFFILIATE%' THEN "" 
          when engine='google' then 'google'
          when engine='bing' then 'bing'
          ELSE a.utm_source 
        END as utm_source,
        b.engine,
        sum(visits) AS visits,
        sum(attributed_receipts) AS attr_receipts,
        sum(attributed_gms) AS attr_gms,
        sum(insession_gms) AS insession_gms,
        sum(insession_orders) AS insession_orders,
        sum(attributed_attr_rev + gcp_costs) AS attributed_rev,
        sum(attributed_new_receipts) AS attributed_new_receipts,
        sum(attributed_new_gms) AS attributed_new_gms,
        sum(attributed_new_attr_rev) AS attributed_new_rev,
        sum(attributed_lapsed_receipts) AS attributed_lapsed_receipts,
        sum(attributed_lapsed_gms) AS attributed_lapsed_gms,
        sum(attributed_lapsed_attr_rev) AS attributed_lapsed_rev,
        sum(attributed_existing_receipts) AS attributed_existing_receipts,
        sum(attributed_existing_gms) AS attributed_existing_gms,
        sum(attributed_existing_attr_rev) AS attributed_existing_rev,
        sum(new_visits) AS new_visits,
        sum(prolist_revenue) AS prolist_revenue,
        sum(gcp_costs) as gcp_costs
        -- metrics with current multiplier
        ,sum(attributed_gms_mult) as attributed_gms_mult
        ,sum(attributed_attr_rev_mult + gcp_costs_mult) as attributed_attr_rev_mult
        ,sum(attributed_receipts_mult) as attributed_receipts_mult
        ,sum(attributed_new_receipts_mult) as attributed_new_receipts_mult
        ,sum(attributed_lapsed_receipts_mult) as attributed_lapsed_receipts_mult
        ,sum(attributed_existing_receipts_mult) as attributed_existing_receipts_mult
        ,sum(attributed_new_gms_mult) as attributed_new_gms_mult
        ,sum(attributed_lapsed_gms_mult) as attributed_lapsed_gms_mult
        ,sum(attributed_existing_gms_mult) as attributed_existing_gms_mult
        ,sum(attributed_new_attr_rev_mult) as attributed_new_attr_rev_mult
        ,sum(attributed_lapsed_attr_rev_mult) as attributed_lapsed_attr_rev_mult
        ,sum(attributed_existing_attr_rev_mult) as attributed_existing_attr_rev_mult
        ,sum(gcp_costs_mult) as gcp_costs_mult
        -- metrics with finance multiplier
        ,sum(attributed_gms_mult_fin) as attributed_gms_mult_fin
        ,sum(attributed_attr_rev_mult_fin + gcp_costs_mult_fin) as attributed_attr_rev_mult_fin
        ,sum(attributed_receipts_mult_fin) as attributed_receipts_mult_fin
        ,sum(attributed_new_receipts_mult_fin) as attributed_new_receipts_mult_fin
        ,sum(attributed_lapsed_receipts_mult_fin) as attributed_lapsed_receipts_mult_fin
        ,sum(attributed_existing_receipts_mult_fin) as attributed_existing_receipts_mult_fin
        ,sum(attributed_new_gms_mult_fin) as attributed_new_gms_mult_fin
        ,sum(attributed_lapsed_gms_mult_fin) as attributed_lapsed_gms_mult_fin
        ,sum(attributed_existing_gms_mult_fin) as attributed_existing_gms_mult_fin
        ,sum(attributed_new_attr_rev_mult_fin) as attributed_new_attr_rev_mult_fin
        ,sum(attributed_lapsed_attr_rev_mult_fin) as attributed_lapsed_attr_rev_mult_fin
        ,sum(attributed_existing_attr_rev_mult_fin) as attributed_existing_attr_rev_mult_fin
        ,sum(gcp_costs_mult_fin) as gcp_costs_mult_fin
      FROM
        `etsy-data-warehouse-dev.buyatt_rollups.channel_overview` a
      left outer join  `etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_channels_def`  b 
        on coalesce(a.utm_campaign,'') = coalesce(b.utm_campaign,'') and 
        coalesce(a.second_channel,'') = coalesce(b.second_channel,'') and 
        coalesce(a.third_channel,'') = coalesce(b.third_channel,'') and     
        coalesce(a.utm_medium,'') = coalesce(b.utm_medium,'') and 
        coalesce(a.utm_source,'') = coalesce(b.utm_source,'') and
        coalesce(a.landing_event,'') = coalesce(b.landing_event,'') and
        coalesce(a.utm_content,'') = coalesce(b.utm_content,'') and
        coalesce(a.marketing_region,'') = coalesce(b.marketing_region,'') 
      WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp', 'pinterest_disp_intl', 'native_display', 'intl_native_display'
      )
       AND extract(YEAR from date) >= 2018
      GROUP BY 1, 2, 3, 4, 5, 6
      ORDER BY 1 DESC NULLS FIRST, 3 DESC
  );

-- channel overview purchase date with account name created to merge the internal and external data sources                                                                                      
CREATE TEMPORARY TABLE internal_purchase as 
  (

    SELECT
        a.purchase_date AS date,
        b.channel,
        case WHEN upper(a.third_channel) LIKE '%AFFILIATE%' THEN "" else a.utm_custom2 end as utm_custom2,
        CASE
          when b.engine='google' and a.utm_medium not like '%video%' then 'cpc'
          when b.engine='bing' and a.utm_medium not like '%video%' then 'cpc'
          WHEN upper(a.third_channel) LIKE '%AFFILIATE%' THEN ""
          ELSE a.utm_medium
        END AS utm_medium,
        CASE WHEN upper(a.third_channel) LIKE '%AFFILIATE%' THEN "" 
          when b.engine='google' then 'google'
          when b.engine='bing' then 'bing'
          ELSE a.utm_source 
        END as utm_source,
        b.engine
        ,sum(attributed_receipts) AS attr_receipts_purch_date
        ,sum(attributed_gms) AS attr_gms_purch_date
        ,sum(attributed_attr_rev) AS attributed_rev_purch_date
        ,sum(attributed_new_receipts) AS attributed_new_receipts_purch_date
        ,sum(attributed_new_gms) AS attributed_new_gms_purch_date
        ,sum(attributed_new_attr_rev) AS attributed_new_rev_purch_date
        ,sum(attributed_lapsed_receipts) AS attributed_lapsed_receipts_purch_date
        ,sum(attributed_lapsed_gms) AS attributed_lapsed_gms_purch_date
        ,sum(attributed_lapsed_attr_rev) AS attributed_lapsed_rev_purch_date
        ,sum(attributed_existing_receipts) AS attributed_existing_receipts_purch_date
        ,sum(attributed_existing_gms) AS attributed_existing_gms_purch_date
        ,sum(attributed_existing_attr_rev) AS attributed_existing_rev_purch_date
        ,sum(attributed_gms_mult) as attributed_gms_mult_purch_date
        ,sum(attributed_attr_rev_mult) as attributed_attr_rev_mult_purch_date
        ,sum(attributed_receipts_mult) as attributed_receipts_mult_purch_date
        ,sum(attributed_new_receipts_mult) as attributed_new_receipts_mult_purch_date
        ,sum(attributed_lapsed_receipts_mult) as attributed_lapsed_receipts_mult_purch_date
        ,sum(attributed_existing_receipts_mult) as attributed_existing_receipts_mult_purch_date
        ,sum(attributed_new_gms_mult) as attributed_new_gms_mult_purch_date
        ,sum(attributed_lapsed_gms_mult) as attributed_lapsed_gms_mult_purch_date
        ,sum(attributed_existing_gms_mult) as attributed_existing_gms_mult_purch_date
        ,sum(attributed_new_attr_rev_mult) as attributed_new_attr_rev_mult_purch_date
        ,sum(attributed_lapsed_attr_rev_mult) as attributed_lapsed_attr_rev_mult_purch_date
        ,sum(attributed_existing_attr_rev_mult) as attributed_existing_attr_rev_mult_purch_date
        ,sum(attributed_gms_mult_fin) as attributed_gms_mult_fin_purch_date
        ,sum(attributed_attr_rev_mult_fin) as attributed_attr_rev_mult_fin_purch_date
        ,sum(attributed_receipts_mult_fin) as attributed_receipts_mult_fin_purch_date
        ,sum(attributed_new_receipts_mult_fin) as attributed_new_receipts_mult_fin_purch_date
        ,sum(attributed_lapsed_receipts_mult_fin) as attributed_lapsed_receipts_mult_fin_purch_date
        ,sum(attributed_existing_receipts_mult_fin) as attributed_existing_receipts_mult_fin_purch_date
        ,sum(attributed_new_gms_mult_fin) as attributed_new_gms_mult_fin_purch_date
        ,sum(attributed_lapsed_gms_mult_fin) as attributed_lapsed_gms_mult_fin_purch_date
        ,sum(attributed_existing_gms_mult_fin) as attributed_existing_gms_mult_fin_purch_date
        ,sum(attributed_new_attr_rev_mult_fin) as attributed_new_attr_rev_mult_fin_purch_date
        ,sum(attributed_lapsed_attr_rev_mult_fin) as attributed_lapsed_attr_rev_mult_fin_purch_date
        ,sum(attributed_existing_attr_rev_mult_fin) as attributed_existing_attr_rev_mult_fin_purch_date

        --adjusted metrics 

        --for adjusted revenue, the adjustment factor is applied to the revenue without prolist
        ,sum(((attributed_attr_rev - prolist_revenue) * coalesce(adjustment_factor,1)) + prolist_revenue) AS attributed_rev_est
        ,sum(attributed_receipts*coalesce(adjustment_factor,1)) AS attr_receipts_est
        ,sum(attributed_gms*coalesce(adjustment_factor,1)) AS attr_gms_est
        ,sum(attributed_new_receipts*coalesce(adjustment_factor,1)) AS attributed_new_receipts_adjusted
        ,sum(attributed_new_gms*coalesce(adjustment_factor,1)) AS attributed_new_gms_adjusted
        ,sum(attributed_new_attr_rev*coalesce(adjustment_factor,1)) AS attributed_new_rev_adjusted
        ,sum(attributed_lapsed_receipts*coalesce(adjustment_factor,1)) AS attributed_lapsed_receipts_adjusted
        ,sum(attributed_lapsed_gms*coalesce(adjustment_factor,1)) AS attributed_lapsed_gms_adjusted
        ,sum(attributed_lapsed_attr_rev*coalesce(adjustment_factor,1)) AS attributed_lapsed_rev_adjusted
        ,sum(attributed_existing_receipts*coalesce(adjustment_factor,1)) AS attributed_existing_receipts_adjusted
        ,sum(attributed_existing_gms*coalesce(adjustment_factor,1)) AS attributed_existing_gms_adjusted
        ,sum(attributed_existing_attr_rev*coalesce(adjustment_factor,1)) AS attributed_existing_rev_adjusted
        
        --adjusted metrics with current multiplier

        --for adjusted revenue with a multiplier, the adjustment factor is applied to the revenue without prolist, but the multiplier applies to the total revenue, including prolist
        ,sum((((attributed_attr_rev_mult/nullif(incrementality_multiplier_current,0))-prolist_revenue)*(coalesce(adjustment_factor,1))+prolist_revenue)*incrementality_multiplier_current) as attributed_attr_rev_adjusted_mult
        ,sum(attributed_gms_mult*coalesce(adjustment_factor,1)) as attributed_gms_adjusted_mult
        ,sum(attributed_receipts_mult*coalesce(adjustment_factor,1)) as attributed_receipts_adjusted_mult
        ,sum(attributed_new_receipts_mult*coalesce(adjustment_factor,1)) as attributed_new_receipts_adjusted_mult
        ,sum(attributed_lapsed_receipts_mult*coalesce(adjustment_factor,1)) as attributed_lapsed_receipts_adjusted_mult
        ,sum(attributed_existing_receipts_mult*coalesce(adjustment_factor,1)) as attributed_existing_receipts_adjusted_mult
        ,sum(attributed_new_gms_mult*coalesce(adjustment_factor,1)) as attributed_new_gms_adjusted_mult
        ,sum(attributed_lapsed_gms_mult*coalesce(adjustment_factor,1)) as attributed_lapsed_gms_adjusted_mult
        ,sum(attributed_existing_gms_mult*coalesce(adjustment_factor,1)) as attributed_existing_gms_adjusted_mult
        ,sum(attributed_new_attr_rev_mult*coalesce(adjustment_factor,1)) as attributed_new_attr_rev_adjusted_mult
        ,sum(attributed_lapsed_attr_rev_mult*coalesce(adjustment_factor,1)) as attributed_lapsed_attr_rev_adjusted_mult
        ,sum(attributed_existing_attr_rev_mult*coalesce(adjustment_factor,1)) as attributed_existing_attr_rev_adjusted_mult
        
        --adjusted metrics with finance multiplier

        --for adjusted revenue with a multiplier, the adjustment factor is applied to the revenue without prolist, but the multiplier applies to the total revenue, including prolist
        ,sum(((((attributed_attr_rev_mult_fin/nullif(incrementality_multiplier_finance,0))-prolist_revenue)*coalesce(adjustment_factor,1))+prolist_revenue)*incrementality_multiplier_finance) as attributed_attr_rev_adjusted_mult_fin
        ,sum(attributed_gms_mult_fin*coalesce(adjustment_factor,1)) as attributed_gms_adjusted_mult_fin
        ,sum(attributed_receipts_mult_fin*coalesce(adjustment_factor,1)) as attributed_receipts_adjusted_mult_fin
        ,sum(attributed_new_receipts_mult_fin*coalesce(adjustment_factor,1)) as attributed_new_receipts_adjusted_mult_fin
        ,sum(attributed_lapsed_receipts_mult_fin*coalesce(adjustment_factor,1)) as attributed_lapsed_receipts_adjusted_mult_fin
        ,sum(attributed_existing_receipts_mult_fin*coalesce(adjustment_factor,1)) as attributed_existing_receipts_adjusted_mult_fin
        ,sum(attributed_new_gms_mult_fin*coalesce(adjustment_factor,1)) as attributed_new_gms_adjusted_mult_fin
        ,sum(attributed_lapsed_gms_mult_fin*coalesce(adjustment_factor,1)) as attributed_lapsed_gms_adjusted_mult_fin
        ,sum(attributed_existing_gms_mult_fin*coalesce(adjustment_factor,1)) as attributed_existing_gms_adjusted_mult_fin
        ,sum(attributed_new_attr_rev_mult_fin*coalesce(adjustment_factor,1)) as attributed_new_attr_rev_adjusted_mult_fin
        ,sum(attributed_lapsed_attr_rev_mult_fin*coalesce(adjustment_factor,1)) as attributed_lapsed_attr_rev_adjusted_mult_fin
        ,sum(attributed_existing_attr_rev_mult_fin*coalesce(adjustment_factor,1)) as attributed_existing_attr_rev_adjusted_mult_fin
      FROM
        `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date` a
        left outer join  `etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_channels_def` b 
        on coalesce(a.utm_campaign,'') = coalesce(b.utm_campaign,'') and 
        coalesce(a.second_channel,'') = coalesce(b.second_channel,'') and 
        coalesce(a.third_channel,'') = coalesce(b.third_channel,'') and     
        coalesce(a.utm_medium,'') = coalesce(b.utm_medium,'') and 
        coalesce(a.utm_source,'') = coalesce(b.utm_source,'') and
        coalesce(a.landing_event,'') = coalesce(b.landing_event,'') and
        coalesce(a.utm_content,'') = coalesce(b.utm_content,'') and
        coalesce(a.marketing_region,'') = coalesce(b.marketing_region,'') 
        left outer join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` cd
        on cd.top_channel = coalesce(a.top_channel, '')
        and cd.second_channel = coalesce(a.second_channel, b.second_channel)
        and cd.third_channel = coalesce(a.third_channel, b.third_channel)
        and cd.utm_campaign = coalesce(a.utm_campaign, b.utm_campaign)
        and cd.utm_medium = coalesce(a.utm_medium, b.utm_medium)
        left join `etsy-data-warehouse-prod.buyatt_mart.latency_adjustments` la
        on la.reporting_channel_group = cd.reporting_channel_group
        and la.marketing_region = (case when (coalesce(a.marketing_region, b.marketing_region)) in ('US','GB','DE') then (coalesce(a.marketing_region, b.marketing_region)) else 'ROW' end)
        and la.date = a.purchase_date
      WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp', 'pinterest_disp_intl', 'native_display', 'intl_native_display'
      )
       AND extract(YEAR from purchase_date) >= 2018
      GROUP BY 1, 2, 3, 4, 5, 6
      ORDER BY  1 DESC NULLS FIRST, 3 DESC
  );

-- merge external marketing data to internal data by date, account name, and campaign id/utm custom 2 
-- coalesces are used to ensure that fields like day, account_name, campaign_id, etc. are always populated especially in the event that a campaign has had no clicks on a given day but has generated gms.                                              
CREATE OR REPLACE TEMPORARY TABLE performance_marketing_daily_tracker_temp
  AS WITH total AS (
    SELECT
        coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) AS day,
        coalesce(coalesce(a.account_name, b.channel), c.channel) AS account_name,
        coalesce(coalesce(CAST(a.campaign_id as STRING), b.utm_custom2), c.utm_custom2) AS campaign_id,
        coalesce(coalesce(a.campaign_name, d.campaign_name), e.campaign_name) AS campaign_name,
        coalesce(coalesce(a.engine, b.engine), c.engine) AS engine,
        coalesce(b.utm_medium, c.utm_medium) as utm_medium,
        coalesce(b.utm_source, c.utm_source) as utm_source,                                                                          
        a.clicks,
        a.cost,
        a.impressions,
        b.visits,
        b.attr_receipts,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attr_receipts_est else b.attr_receipts end as attr_receipts_est,
        b.attr_gms,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attr_gms_est else b.attr_gms end as attr_gms_est,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then coalesce(c.attributed_rev_est,0) - coalesce(b.gcp_costs,0) else coalesce(b.attributed_rev,0) - coalesce(b.gcp_costs,0) end as attr_rev_est,
        b.insession_gms,
        b.insession_orders,
        coalesce(b.attributed_rev,0) - coalesce(b.gcp_costs,0) as attributed_rev,
        b.attributed_new_receipts,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_receipts_adjusted else b.attributed_new_receipts end as attributed_new_receipts_adjusted,
        b.attributed_new_gms,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_gms_adjusted else b.attributed_new_gms end as attributed_new_gms_adjusted,
        b.attributed_new_rev,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_rev_adjusted else b.attributed_new_rev end as attributed_new_rev_adjusted,
        b.attributed_lapsed_receipts,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_receipts_adjusted else b.attributed_lapsed_receipts end as attributed_lapsed_receipts_adjusted,
        b.attributed_lapsed_gms,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_gms_adjusted else b.attributed_lapsed_gms end as attributed_lapsed_gms_adjusted,
        b.attributed_lapsed_rev,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_rev_adjusted else b.attributed_lapsed_rev end as attributed_lapsed_rev_adjusted,
        b.attributed_existing_receipts,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_receipts_adjusted else b.attributed_existing_receipts end as attributed_existing_receipts_adjusted,
        b.attributed_existing_gms,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_gms_adjusted else b.attributed_existing_gms end as attributed_existing_gms_adjusted,
        b.attributed_existing_rev,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_rev_adjusted else b.attributed_existing_rev end as attributed_existing_rev_adjusted,
        b.new_visits,
        b.prolist_revenue,
        b.attributed_gms_mult,
        coalesce(b.attributed_attr_rev_mult,0) - coalesce(b.gcp_costs_mult,0) as attributed_attr_rev_mult,
        b.attributed_receipts_mult,
        b.attributed_new_receipts_mult,
        b.attributed_lapsed_receipts_mult,
        b.attributed_existing_receipts_mult,
        b.attributed_new_gms_mult,
        b.attributed_lapsed_gms_mult,
        b.attributed_existing_gms_mult,
        b.attributed_new_attr_rev_mult,
        b.attributed_lapsed_attr_rev_mult,
        b.attributed_existing_attr_rev_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_gms_adjusted_mult else b.attributed_gms_mult end as attributed_gms_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then coalesce(c.attributed_attr_rev_adjusted_mult,0) - coalesce(b.gcp_costs_mult,0) else coalesce(b.attributed_attr_rev_mult,0) - coalesce(b.gcp_costs_mult,0) end as attributed_attr_rev_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_receipts_adjusted_mult else b.attributed_receipts_mult end as attributed_receipts_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_receipts_adjusted_mult else b.attributed_new_receipts_mult end as attributed_new_receipts_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_receipts_adjusted_mult else b.attributed_lapsed_receipts_mult end as attributed_lapsed_receipts_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_receipts_adjusted_mult else b.attributed_existing_receipts_mult end as attributed_existing_receipts_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_gms_adjusted_mult else b.attributed_new_gms_mult end as attributed_new_gms_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_gms_adjusted_mult else b.attributed_lapsed_gms_mult end as attributed_lapsed_gms_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_gms_adjusted_mult else b.attributed_existing_gms_mult end as attributed_existing_gms_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_attr_rev_adjusted_mult else b.attributed_new_attr_rev_mult end as attributed_new_attr_rev_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_attr_rev_adjusted_mult else b.attributed_lapsed_attr_rev_mult end as attributed_lapsed_attr_rev_adjusted_mult,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_attr_rev_adjusted_mult else b.attributed_existing_attr_rev_mult end as attributed_existing_attr_rev_adjusted_mult,
        b.attributed_gms_mult_fin,
        coalesce(b.attributed_attr_rev_mult_fin,0) - coalesce(b.gcp_costs_mult_fin,0) as attributed_attr_rev_mult_fin,
        b.attributed_receipts_mult_fin,
        b.attributed_new_receipts_mult_fin,
        b.attributed_lapsed_receipts_mult_fin,
        b.attributed_existing_receipts_mult_fin,
        b.attributed_new_gms_mult_fin,
        b.attributed_lapsed_gms_mult_fin,
        b.attributed_existing_gms_mult_fin,
        b.attributed_new_attr_rev_mult_fin,
        b.attributed_lapsed_attr_rev_mult_fin,
        b.attributed_existing_attr_rev_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_gms_adjusted_mult_fin else b.attributed_gms_mult_fin end as attributed_gms_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then coalesce(c.attributed_attr_rev_adjusted_mult_fin,0) - coalesce(b.gcp_costs_mult_fin,0) else coalesce(b.attributed_attr_rev_mult_fin,0) - coalesce(b.gcp_costs_mult_fin,0) end as attributed_attr_rev_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_receipts_adjusted_mult_fin else b.attributed_receipts_mult_fin end as attributed_receipts_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_receipts_adjusted_mult_fin else b.attributed_new_receipts_mult_fin end as attributed_new_receipts_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_receipts_adjusted_mult_fin else b.attributed_lapsed_receipts_mult_fin end as attributed_lapsed_receipts_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_receipts_adjusted_mult_fin else b.attributed_existing_receipts_mult_fin end as attributed_existing_receipts_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_gms_adjusted_mult_fin else b.attributed_new_gms_mult_fin end as attributed_new_gms_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_gms_adjusted_mult_fin else b.attributed_lapsed_gms_mult_fin end as attributed_lapsed_gms_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_gms_adjusted_mult_fin else b.attributed_existing_gms_mult_fin end as attributed_existing_gms_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_new_attr_rev_adjusted_mult_fin else b.attributed_new_attr_rev_mult_fin end as attributed_new_attr_rev_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_lapsed_attr_rev_adjusted_mult_fin else b.attributed_lapsed_attr_rev_mult_fin end as attributed_lapsed_attr_rev_adjusted_mult_fin,
        case when coalesce(coalesce(cast(a.day as DATETIME ), CAST(b.date as DATETIME)), cast(c.date as DATETIME )) >= current_date() - 30
            then c.attributed_existing_attr_rev_adjusted_mult_fin else b.attributed_existing_attr_rev_mult_fin end as attributed_existing_attr_rev_adjusted_mult_fin,
        b.gcp_costs_mult,
        b.gcp_costs_mult_fin,
        c.attr_receipts_purch_date,
        c.attr_gms_purch_date,
        coalesce(c.attributed_rev_purch_date,0) - coalesce(b.gcp_costs_mult,0) as attributed_rev_purch_date,
        c.attributed_new_receipts_purch_date,
        c.attributed_new_gms_purch_date,
        c.attributed_new_rev_purch_date,
        c.attributed_lapsed_receipts_purch_date,
        c.attributed_lapsed_gms_purch_date,
        c.attributed_lapsed_rev_purch_date,
        c.attributed_existing_receipts_purch_date,
        c.attributed_existing_gms_purch_date,
        c.attributed_existing_rev_purch_date,
        c.attributed_gms_mult_purch_date,
        coalesce(c.attributed_attr_rev_mult_purch_date,0) - coalesce(b.gcp_costs_mult,0) as attributed_attr_rev_mult_purch_date,
        c.attributed_receipts_mult_purch_date,
        c.attributed_new_receipts_mult_purch_date,
        c.attributed_lapsed_receipts_mult_purch_date,
        c.attributed_existing_receipts_mult_purch_date,
        c.attributed_new_gms_mult_purch_date,
        c.attributed_lapsed_gms_mult_purch_date,
        c.attributed_existing_gms_mult_purch_date,
        c.attributed_new_attr_rev_mult_purch_date,
        c.attributed_lapsed_attr_rev_mult_purch_date,
        c.attributed_existing_attr_rev_mult_purch_date,
        c.attributed_gms_mult_fin_purch_date,
        coalesce(c.attributed_attr_rev_mult_fin_purch_date,0) - coalesce(b.gcp_costs_mult_fin,0) as attributed_attr_rev_mult_fin_purch_date,
        c.attributed_receipts_mult_fin_purch_date,
        c.attributed_new_receipts_mult_fin_purch_date,
        c.attributed_lapsed_receipts_mult_fin_purch_date,
        c.attributed_existing_receipts_mult_fin_purch_date,
        c.attributed_new_gms_mult_fin_purch_date,
        c.attributed_lapsed_gms_mult_fin_purch_date,
        c.attributed_existing_gms_mult_fin_purch_date,
        c.attributed_new_attr_rev_mult_fin_purch_date,
        c.attributed_lapsed_attr_rev_mult_fin_purch_date,
        c.attributed_existing_attr_rev_mult_fin_purch_date
      FROM
        all_markt AS a
        FULL OUTER JOIN internal AS b ON CAST(a.day as DATETIME) = cast(b.date as datetime)
         AND a.account_name = b.channel 
         AND coalesce(CAST(a.campaign_id as STRING),"") = b.utm_custom2
        FULL OUTER JOIN internal_purchase AS c ON CAST(a.day as DATETIME) = cast(c.date as datetime)
         AND a.account_name = c.channel
         AND coalesce(CAST(a.campaign_id as STRING),"") = c.utm_custom2
        -- this pulls the latest campaign name associated with a campaign id for days certain campaign have no clicks but gms (click date)                                                                            
        LEFT OUTER JOIN  all_markt_campaign_name d on d.account_name = b.channel
         AND coalesce(CAST(d.campaign_id as STRING),"") = b.utm_custom2
        -- this pulls the latest campaign name associated with a campaign id for days certain campaign have no clicks but gms (purchase date)                                                                                                                                                  
        LEFT OUTER JOIN  all_markt_campaign_name e on e.account_name = c.channel
         AND coalesce(CAST(e.campaign_id as STRING),"") = c.utm_custom2
          --ORDER BY 1 DESC NULLS FIRST, 2 NULLS LAST
  )
  SELECT
      total.day,
      total.account_name,
      total.campaign_id,
      total.campaign_name,
      total.engine,
      total.utm_medium,
      total.utm_source,
      total.clicks,
      total.cost,
      total.impressions,
      total.visits,
      total.attr_receipts,
      total.attr_receipts_est,
      total.attr_gms,
      total.attr_gms_est,
      total.attr_rev_est,
      total.insession_gms,
      total.insession_orders,
      total.attributed_rev,
      total.attributed_new_receipts,
      total.attributed_new_receipts_adjusted,
      total.attributed_new_gms,
      total.attributed_new_gms_adjusted,
      total.attributed_new_rev,
      total.attributed_new_rev_adjusted,
      total.attributed_lapsed_receipts,
      total.attributed_lapsed_receipts_adjusted,
      total.attributed_lapsed_gms,
      total.attributed_lapsed_gms_adjusted,
      total.attributed_lapsed_rev,
      total.attributed_lapsed_rev_adjusted,
      total.attributed_existing_receipts,
      total.attributed_existing_receipts_adjusted,
      total.attributed_existing_gms,
      total.attributed_existing_gms_adjusted,
      total.attributed_existing_rev,
      total.attributed_existing_rev_adjusted,
      total.new_visits,
      total.prolist_revenue,
      total.attributed_gms_mult,
      total.attributed_attr_rev_mult,
      total.attributed_receipts_mult,
      total.attributed_new_receipts_mult,
      total.attributed_lapsed_receipts_mult,
      total.attributed_existing_receipts_mult,
      total.attributed_new_gms_mult,
      total.attributed_lapsed_gms_mult,
      total.attributed_existing_gms_mult,
      total.attributed_new_attr_rev_mult,
      total.attributed_lapsed_attr_rev_mult,
      total.attributed_existing_attr_rev_mult,
      total.attributed_gms_adjusted_mult,
      total.attributed_attr_rev_adjusted_mult,
      total.attributed_receipts_adjusted_mult,
      total.attributed_new_receipts_adjusted_mult,
      total.attributed_lapsed_receipts_adjusted_mult,
      total.attributed_existing_receipts_adjusted_mult,
      total.attributed_new_gms_adjusted_mult,
      total.attributed_lapsed_gms_adjusted_mult,
      total.attributed_existing_gms_adjusted_mult,
      total.attributed_new_attr_rev_adjusted_mult,
      total.attributed_lapsed_attr_rev_adjusted_mult,
      total.attributed_existing_attr_rev_adjusted_mult,
      total.attributed_gms_mult_fin,
      total.attributed_attr_rev_mult_fin,
      total.attributed_receipts_mult_fin,
      total.attributed_new_receipts_mult_fin,
      total.attributed_lapsed_receipts_mult_fin,
      total.attributed_existing_receipts_mult_fin,
      total.attributed_new_gms_mult_fin,
      total.attributed_lapsed_gms_mult_fin,
      total.attributed_existing_gms_mult_fin,
      total.attributed_new_attr_rev_mult_fin,
      total.attributed_lapsed_attr_rev_mult_fin,
      total.attributed_existing_attr_rev_mult_fin,
      total.attributed_gms_adjusted_mult_fin,
      total.attributed_attr_rev_adjusted_mult_fin,
      total.attributed_receipts_adjusted_mult_fin,
      total.attributed_new_receipts_adjusted_mult_fin,
      total.attributed_lapsed_receipts_adjusted_mult_fin,
      total.attributed_existing_receipts_adjusted_mult_fin,
      total.attributed_new_gms_adjusted_mult_fin,
      total.attributed_lapsed_gms_adjusted_mult_fin,
      total.attributed_existing_gms_adjusted_mult_fin,
      total.attributed_new_attr_rev_adjusted_mult_fin,
      total.attributed_lapsed_attr_rev_adjusted_mult_fin,
      total.attributed_existing_attr_rev_adjusted_mult_fin,
      total.attr_receipts_purch_date,
      total.attr_gms_purch_date,
      total.attributed_rev_purch_date,
      total.attributed_new_receipts_purch_date,
      total.attributed_new_gms_purch_date,
      total.attributed_new_rev_purch_date,
      total.attributed_lapsed_receipts_purch_date,
      total.attributed_lapsed_gms_purch_date,
      total.attributed_lapsed_rev_purch_date,
      total.attributed_existing_receipts_purch_date,
      total.attributed_existing_gms_purch_date,
      total.attributed_existing_rev_purch_date,
      total.attributed_gms_mult_purch_date,
      total.attributed_attr_rev_mult_purch_date,
      total.attributed_receipts_mult_purch_date,
      total.attributed_new_receipts_mult_purch_date,
      total.attributed_lapsed_receipts_mult_purch_date,
      total.attributed_existing_receipts_mult_purch_date,
      total.attributed_new_gms_mult_purch_date,
      total.attributed_lapsed_gms_mult_purch_date,
      total.attributed_existing_gms_mult_purch_date,
      total.attributed_new_attr_rev_mult_purch_date,
      total.attributed_lapsed_attr_rev_mult_purch_date,
      total.attributed_existing_attr_rev_mult_purch_date,
      total.attributed_gms_mult_fin_purch_date,
      total.attributed_attr_rev_mult_fin_purch_date,
      total.attributed_receipts_mult_fin_purch_date,
      total.attributed_new_receipts_mult_fin_purch_date,
      total.attributed_lapsed_receipts_mult_fin_purch_date,
      total.attributed_existing_receipts_mult_fin_purch_date,
      total.attributed_new_gms_mult_fin_purch_date,
      total.attributed_lapsed_gms_mult_fin_purch_date,
      total.attributed_existing_gms_mult_fin_purch_date,
      total.attributed_new_attr_rev_mult_fin_purch_date,
      total.attributed_lapsed_attr_rev_mult_fin_purch_date,
      total.attributed_existing_attr_rev_mult_fin_purch_date,
      total.gcp_costs_mult,
      total.gcp_costs_mult_fin,
    FROM
      total
;
-- create final table with additional columns describing performance 1 year ago, 52 weeks ago, and 104 weeks ago for year over year metrics in Looker                                                       
CREATE OR REPLACE TEMPORARY TABLE performance_marketing_daily_tracker_final
  AS  WITH c AS (
    SELECT
        performance_marketing_daily_tracker_temp.day,
        performance_marketing_daily_tracker_temp.account_name,
        CAST(performance_marketing_daily_tracker_temp.campaign_id as STRING) AS campaign_id,
        performance_marketing_daily_tracker_temp.campaign_name as campaign_name,
        performance_marketing_daily_tracker_temp.engine,
        performance_marketing_daily_tracker_temp.utm_medium,
        performance_marketing_daily_tracker_temp.utm_source,
        performance_marketing_daily_tracker_temp.clicks,
        performance_marketing_daily_tracker_temp.cost,
        performance_marketing_daily_tracker_temp.impressions,
        performance_marketing_daily_tracker_temp.visits,
        performance_marketing_daily_tracker_temp.attr_receipts,
        performance_marketing_daily_tracker_temp.attr_receipts_est,
        performance_marketing_daily_tracker_temp.attr_gms,
        performance_marketing_daily_tracker_temp.attr_gms_est,
        performance_marketing_daily_tracker_temp.attr_rev_est,
        performance_marketing_daily_tracker_temp.insession_gms,
        performance_marketing_daily_tracker_temp.insession_orders,
        performance_marketing_daily_tracker_temp.attributed_rev,
        performance_marketing_daily_tracker_temp.attributed_new_receipts,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_adjusted,
        performance_marketing_daily_tracker_temp.attributed_new_gms,
        performance_marketing_daily_tracker_temp.attributed_new_gms_adjusted,
        performance_marketing_daily_tracker_temp.attributed_new_rev,
        performance_marketing_daily_tracker_temp.attributed_new_rev_adjusted,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_adjusted,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_adjusted,
        performance_marketing_daily_tracker_temp.attributed_lapsed_rev,
        performance_marketing_daily_tracker_temp.attributed_lapsed_rev_adjusted,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_adjusted,
        performance_marketing_daily_tracker_temp.attributed_existing_gms,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_adjusted,
        performance_marketing_daily_tracker_temp.attributed_existing_rev,
        performance_marketing_daily_tracker_temp.attributed_existing_rev_adjusted,
        performance_marketing_daily_tracker_temp.new_visits,
        performance_marketing_daily_tracker_temp.prolist_revenue,
        performance_marketing_daily_tracker_temp.attr_receipts_purch_date,
        performance_marketing_daily_tracker_temp.attr_gms_purch_date,
        performance_marketing_daily_tracker_temp.attributed_rev_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_gms_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_rev_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_rev_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_rev_purch_date,
        -- multiplier values
        performance_marketing_daily_tracker_temp.attributed_gms_mult,
        performance_marketing_daily_tracker_temp.attributed_attr_rev_mult,
        performance_marketing_daily_tracker_temp.attributed_receipts_mult,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_mult,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_mult,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_mult,
        performance_marketing_daily_tracker_temp.attributed_new_gms_mult,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_mult,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_mult,
        performance_marketing_daily_tracker_temp.attributed_new_attr_rev_mult,
        performance_marketing_daily_tracker_temp.attributed_lapsed_attr_rev_mult,
        performance_marketing_daily_tracker_temp.attributed_existing_attr_rev_mult,
        performance_marketing_daily_tracker_temp.attributed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_new_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_new_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_lapsed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_existing_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp.attributed_gms_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_new_gms_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_new_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_lapsed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_existing_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_new_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_new_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_lapsed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp.attributed_existing_attr_rev_adjusted_mult_fin,
        -- multiplier values purch date
        performance_marketing_daily_tracker_temp.attributed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp.attributed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_new_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_lapsed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp.attributed_existing_attr_rev_mult_fin_purch_date,
        -- gcp costs
        performance_marketing_daily_tracker_temp.gcp_costs_mult,
        performance_marketing_daily_tracker_temp.gcp_costs_mult_fin,
        'a' AS flag
      FROM
        performance_marketing_daily_tracker_temp
    UNION ALL
    SELECT
        datetime_add(performance_marketing_daily_tracker_temp_0.day, interval 1 YEAR) AS day,
        performance_marketing_daily_tracker_temp_0.account_name,
        CAST(performance_marketing_daily_tracker_temp_0.campaign_id as STRING) AS campaign_id,
        performance_marketing_daily_tracker_temp_0.campaign_name as campaign_name,
        performance_marketing_daily_tracker_temp_0.engine,
        performance_marketing_daily_tracker_temp_0.utm_medium,
        performance_marketing_daily_tracker_temp_0.utm_source,
        performance_marketing_daily_tracker_temp_0.clicks,
        performance_marketing_daily_tracker_temp_0.cost,
        performance_marketing_daily_tracker_temp_0.impressions,
        performance_marketing_daily_tracker_temp_0.visits,
        performance_marketing_daily_tracker_temp_0.attr_receipts,
        performance_marketing_daily_tracker_temp_0.attr_receipts_est,
        performance_marketing_daily_tracker_temp_0.attr_gms,
        performance_marketing_daily_tracker_temp_0.attr_gms_est,
        performance_marketing_daily_tracker_temp_0.attr_rev_est,
        performance_marketing_daily_tracker_temp_0.insession_gms,
        performance_marketing_daily_tracker_temp_0.insession_orders,
        performance_marketing_daily_tracker_temp_0.attributed_rev,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_new_rev,
        performance_marketing_daily_tracker_temp_0.attributed_new_rev_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_rev,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_rev_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_adjusted,
        performance_marketing_daily_tracker_temp_0.attributed_existing_rev,
        performance_marketing_daily_tracker_temp_0.attributed_existing_rev_adjusted,
        performance_marketing_daily_tracker_temp_0.new_visits,
        performance_marketing_daily_tracker_temp_0.prolist_revenue,
        performance_marketing_daily_tracker_temp_0.attr_receipts_purch_date,
        performance_marketing_daily_tracker_temp_0.attr_gms_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_rev_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_rev_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_rev_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_rev_purch_date,
        -- multiplier values
        performance_marketing_daily_tracker_temp_0.attributed_gms_mult,
        performance_marketing_daily_tracker_temp_0.attributed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_0.attributed_receipts_mult,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_mult,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_mult,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_mult,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_mult,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_mult,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_mult,
        performance_marketing_daily_tracker_temp_0.attributed_new_attr_rev_mult,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_0.attributed_existing_attr_rev_mult,
        performance_marketing_daily_tracker_temp_0.attributed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_new_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_existing_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_0.attributed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_new_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_existing_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_new_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_0.attributed_existing_attr_rev_adjusted_mult_fin,
        -- multiplier values purch date
        performance_marketing_daily_tracker_temp_0.attributed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_new_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_lapsed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_0.attributed_existing_attr_rev_mult_fin_purch_date,
        -- gcp costs
        performance_marketing_daily_tracker_temp_0.gcp_costs_mult,
        performance_marketing_daily_tracker_temp_0.gcp_costs_mult_fin,
        'b' AS flag
      FROM
        performance_marketing_daily_tracker_temp AS performance_marketing_daily_tracker_temp_0
      WHERE datetime_add(performance_marketing_daily_tracker_temp_0.day, interval 1 YEAR) < CAST(current_date() as DATETIME)
    UNION ALL
    SELECT
        datetime_add(performance_marketing_daily_tracker_temp_1.day, interval 52 WEEK) AS day,
        performance_marketing_daily_tracker_temp_1.account_name,
        CAST(performance_marketing_daily_tracker_temp_1.campaign_id as STRING) AS campaign_id,
        performance_marketing_daily_tracker_temp_1.campaign_name as campaign_name,
        performance_marketing_daily_tracker_temp_1.engine,
        performance_marketing_daily_tracker_temp_1.utm_medium,
        performance_marketing_daily_tracker_temp_1.utm_source,
        performance_marketing_daily_tracker_temp_1.clicks,
        performance_marketing_daily_tracker_temp_1.cost,
        performance_marketing_daily_tracker_temp_1.impressions,
        performance_marketing_daily_tracker_temp_1.visits,
        performance_marketing_daily_tracker_temp_1.attr_receipts,
        performance_marketing_daily_tracker_temp_1.attr_receipts_est,
        performance_marketing_daily_tracker_temp_1.attr_gms,
        performance_marketing_daily_tracker_temp_1.attr_gms_est,
        performance_marketing_daily_tracker_temp_1.attr_rev_est,
        performance_marketing_daily_tracker_temp_1.insession_gms,
        performance_marketing_daily_tracker_temp_1.insession_orders,
        performance_marketing_daily_tracker_temp_1.attributed_rev,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_new_rev,
        performance_marketing_daily_tracker_temp_1.attributed_new_rev_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_rev,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_rev_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_adjusted,
        performance_marketing_daily_tracker_temp_1.attributed_existing_rev,
        performance_marketing_daily_tracker_temp_1.attributed_existing_rev_adjusted,
        performance_marketing_daily_tracker_temp_1.new_visits,
        performance_marketing_daily_tracker_temp_1.prolist_revenue,
        performance_marketing_daily_tracker_temp_1.attr_receipts_purch_date,
        performance_marketing_daily_tracker_temp_1.attr_gms_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_rev_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_rev_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_rev_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_rev_purch_date,
        -- multiplier values
        performance_marketing_daily_tracker_temp_1.attributed_gms_mult,
        performance_marketing_daily_tracker_temp_1.attributed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_1.attributed_receipts_mult,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_mult,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_mult,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_mult,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_mult,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_mult,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_mult,
        performance_marketing_daily_tracker_temp_1.attributed_new_attr_rev_mult,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_1.attributed_existing_attr_rev_mult,
        performance_marketing_daily_tracker_temp_1.attributed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_new_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_existing_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_1.attributed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_new_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_existing_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_new_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_1.attributed_existing_attr_rev_adjusted_mult_fin,
        -- multiplier values purch date
        performance_marketing_daily_tracker_temp_1.attributed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_new_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_lapsed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_1.attributed_existing_attr_rev_mult_fin_purch_date,
        -- gcp costs
        performance_marketing_daily_tracker_temp_1.gcp_costs_mult,
        performance_marketing_daily_tracker_temp_1.gcp_costs_mult_fin,
        'c' AS flag
      FROM
        performance_marketing_daily_tracker_temp AS performance_marketing_daily_tracker_temp_1
         WHERE datetime_add(performance_marketing_daily_tracker_temp_1.day, interval 52 WEEK) < CAST(current_date() as DATETIME)
    UNION ALL
    SELECT
        datetime_add(performance_marketing_daily_tracker_temp_2.day, interval 104 WEEK) AS day,
        performance_marketing_daily_tracker_temp_2.account_name,
        CAST(performance_marketing_daily_tracker_temp_2.campaign_id as STRING) AS campaign_id,
        performance_marketing_daily_tracker_temp_2.campaign_name as campaign_name,
        performance_marketing_daily_tracker_temp_2.engine,
        performance_marketing_daily_tracker_temp_2.utm_medium,
        performance_marketing_daily_tracker_temp_2.utm_source,
        performance_marketing_daily_tracker_temp_2.clicks,
        performance_marketing_daily_tracker_temp_2.cost,
        performance_marketing_daily_tracker_temp_2.impressions,
        performance_marketing_daily_tracker_temp_2.visits,
        performance_marketing_daily_tracker_temp_2.attr_receipts,
        performance_marketing_daily_tracker_temp_2.attr_receipts_est,
        performance_marketing_daily_tracker_temp_2.attr_gms,
        performance_marketing_daily_tracker_temp_2.attr_gms_est,
        performance_marketing_daily_tracker_temp_2.attr_rev_est,
        performance_marketing_daily_tracker_temp_2.insession_gms,
        performance_marketing_daily_tracker_temp_2.insession_orders,
        performance_marketing_daily_tracker_temp_2.attributed_rev,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_new_rev,
        performance_marketing_daily_tracker_temp_2.attributed_new_rev_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_rev,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_rev_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_adjusted,
        performance_marketing_daily_tracker_temp_2.attributed_existing_rev,
        performance_marketing_daily_tracker_temp_2.attributed_existing_rev_adjusted,
        performance_marketing_daily_tracker_temp_2.new_visits,
        performance_marketing_daily_tracker_temp_2.prolist_revenue,
        performance_marketing_daily_tracker_temp_2.attr_receipts_purch_date,
        performance_marketing_daily_tracker_temp_2.attr_gms_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_rev_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_rev_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_rev_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_rev_purch_date,
        -- multiplier values
        performance_marketing_daily_tracker_temp_2.attributed_gms_mult,
        performance_marketing_daily_tracker_temp_2.attributed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_2.attributed_receipts_mult,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_mult,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_mult,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_mult,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_mult,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_mult,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_mult,
        performance_marketing_daily_tracker_temp_2.attributed_new_attr_rev_mult,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_2.attributed_existing_attr_rev_mult,
        performance_marketing_daily_tracker_temp_2.attributed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_new_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_existing_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_2.attributed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_new_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_existing_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_new_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_2.attributed_existing_attr_rev_adjusted_mult_fin,
        -- multiplier values purch date
        performance_marketing_daily_tracker_temp_2.attributed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_new_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_lapsed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_2.attributed_existing_attr_rev_mult_fin_purch_date,
        -- gcp costs
        performance_marketing_daily_tracker_temp_2.gcp_costs_mult,
        performance_marketing_daily_tracker_temp_2.gcp_costs_mult_fin,
        'd' AS flag
      FROM
        performance_marketing_daily_tracker_temp AS performance_marketing_daily_tracker_temp_2
            WHERE datetime_add(performance_marketing_daily_tracker_temp_2.day, interval 104 WEEK) < CAST(current_date() as DATETIME)
    UNION ALL
    SELECT
        datetime_add(performance_marketing_daily_tracker_temp_3.day, interval 156 WEEK) AS day,
        performance_marketing_daily_tracker_temp_3.account_name,
        CAST(performance_marketing_daily_tracker_temp_3.campaign_id as STRING) AS campaign_id,
        performance_marketing_daily_tracker_temp_3.campaign_name as campaign_name,
        performance_marketing_daily_tracker_temp_3.engine,
        performance_marketing_daily_tracker_temp_3.utm_medium,
        performance_marketing_daily_tracker_temp_3.utm_source,
        performance_marketing_daily_tracker_temp_3.clicks,
        performance_marketing_daily_tracker_temp_3.cost,
        performance_marketing_daily_tracker_temp_3.impressions,
        performance_marketing_daily_tracker_temp_3.visits,
        performance_marketing_daily_tracker_temp_3.attr_receipts,
        performance_marketing_daily_tracker_temp_3.attr_receipts_est,
        performance_marketing_daily_tracker_temp_3.attr_gms,
        performance_marketing_daily_tracker_temp_3.attr_gms_est,
        performance_marketing_daily_tracker_temp_3.attr_rev_est,
        performance_marketing_daily_tracker_temp_3.insession_gms,
        performance_marketing_daily_tracker_temp_3.insession_orders,
        performance_marketing_daily_tracker_temp_3.attributed_rev,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_new_rev,
        performance_marketing_daily_tracker_temp_3.attributed_new_rev_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_rev,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_rev_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_adjusted,
        performance_marketing_daily_tracker_temp_3.attributed_existing_rev,
        performance_marketing_daily_tracker_temp_3.attributed_existing_rev_adjusted,
        performance_marketing_daily_tracker_temp_3.new_visits,
        performance_marketing_daily_tracker_temp_3.prolist_revenue,
        performance_marketing_daily_tracker_temp_3.attr_receipts_purch_date,
        performance_marketing_daily_tracker_temp_3.attr_gms_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_rev_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_rev_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_rev_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_rev_purch_date,
        -- multiplier values
        performance_marketing_daily_tracker_temp_3.attributed_gms_mult,
        performance_marketing_daily_tracker_temp_3.attributed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_3.attributed_receipts_mult,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_mult,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_mult,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_mult,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_mult,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_mult,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_mult,
        performance_marketing_daily_tracker_temp_3.attributed_new_attr_rev_mult,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_attr_rev_mult,
        performance_marketing_daily_tracker_temp_3.attributed_existing_attr_rev_mult,
        performance_marketing_daily_tracker_temp_3.attributed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_new_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_existing_attr_rev_adjusted_mult,
        performance_marketing_daily_tracker_temp_3.attributed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_new_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_existing_attr_rev_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_new_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_attr_rev_adjusted_mult_fin,
        performance_marketing_daily_tracker_temp_3.attributed_existing_attr_rev_adjusted_mult_fin,
        -- multiplier values purch date
        performance_marketing_daily_tracker_temp_3.attributed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_attr_rev_mult_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_receipts_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_gms_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_new_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_lapsed_attr_rev_mult_fin_purch_date,
        performance_marketing_daily_tracker_temp_3.attributed_existing_attr_rev_mult_fin_purch_date,
        -- gcp costs
        performance_marketing_daily_tracker_temp_3.gcp_costs_mult,
        performance_marketing_daily_tracker_temp_3.gcp_costs_mult_fin,
        'e' AS flag
      FROM
        performance_marketing_daily_tracker_temp AS performance_marketing_daily_tracker_temp_3
            WHERE datetime_add(performance_marketing_daily_tracker_temp_3.day, interval 156 WEEK) < CAST(current_date() as DATETIME)
  )
  SELECT
      CAST(c.day as TIMESTAMP) as day,
      c.account_name,
      c.engine,
      CASE
        WHEN upper(c.account_name) LIKE '%NB%'
         OR upper(c.account_name) LIKE '%NON-BRAND%'
         OR upper(c.account_name) LIKE '%DSA%' THEN 'SEM NB'
        WHEN upper(c.account_name) LIKE 'PLA%' THEN 'PLA'
        WHEN upper(c.account_name) LIKE '%DISPLAY%' THEN 'Display'
        WHEN upper(c.account_name) LIKE '% BRAND%' THEN 'SEM Brand'
        WHEN upper(c.account_name) LIKE '%BRANDED%' THEN 'SEM Brand'
        WHEN upper(c.engine) LIKE 'AFFILIATE%' THEN 'Affiliate'
        WHEN c.engine = 'facebook'
         AND upper(c.account_name) LIKE '%DYNAMIC' THEN 'Social Product'
        WHEN c.engine = 'facebook'
         AND upper(c.account_name) LIKE '%CURATED' THEN 'Social Curated'
        WHEN c.engine = 'facebook'
         AND upper(c.account_name) LIKE '%VIDEO%' THEN 'Social Video'
        WHEN c.engine = 'facebook'
          AND upper(c.account_name) LIKE '% ASC %' THEN 'Social Optimized'
        WHEN c.engine = 'pinterest'
         AND upper(c.account_name) LIKE '%PINTEREST%' THEN 'Social Pinterest'         
        when upper(c.account_name) LIKE 'YOUTUBE%' THEN 'YouTube'
        ELSE 'other'
      END AS tactic,
      c.campaign_id,
      c.campaign_name,
      c.utm_medium,
      c.utm_source,
      sum(CASE WHEN c.flag = 'a' THEN c.impressions ELSE 0 END) AS impressions,
      sum(CASE WHEN c.flag = 'a' THEN c.clicks ELSE 0 END) AS clicks,
      sum(CASE WHEN c.flag = 'a' THEN c.cost ELSE CAST(0 as FLOAT64) END) AS cost,
      sum(CASE WHEN c.flag = 'a' THEN c.visits ELSE 0 END) AS visits,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_receipts ELSE CAST(0 as FLOAT64) END) AS attr_receipts,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_receipts_est ELSE CAST(0 as FLOAT64) END) AS attr_receipts_est,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_gms ELSE CAST(0 as NUMERIC) END) AS attr_gms,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_gms_est ELSE CAST(0 as FLOAT64) END) AS attr_gms_est,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_rev_est ELSE CAST(0 as FLOAT64) END) AS attr_rev_est,
      sum(CASE WHEN c.flag = 'b' THEN c.impressions ELSE 0 END) AS impressions_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.clicks ELSE 0 END) AS clicks_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.cost ELSE CAST(0 as FLOAT64) END) AS cost_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.visits ELSE 0 END) AS visits_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attr_receipts ELSE CAST(0 as FLOAT64) END) AS attr_receipts_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attr_gms ELSE CAST(0 as NUMERIC) END) AS attr_gms_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attr_rev_est ELSE CAST(0 as FLOAT64) END) AS attr_rev_ly,
      sum(CASE WHEN c.flag = 'c' THEN c.impressions ELSE 0 END) AS impressions_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.clicks ELSE 0 END) AS clicks_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.cost ELSE CAST(0 as FLOAT64) END) AS cost_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.visits ELSE 0 END) AS visits_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attr_receipts ELSE CAST(0 as FLOAT64) END) AS attr_receipts_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attr_gms ELSE CAST(0 as NUMERIC) END) AS attr_gms_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attr_rev_est ELSE CAST(0 as FLOAT64) END) AS attr_rev_dly,
      sum(CASE WHEN c.flag = 'd' THEN c.impressions ELSE 0 END) AS impressions_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.clicks ELSE 0 END) AS clicks_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.cost ELSE CAST(0 as FLOAT64) END) AS cost_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.visits ELSE 0 END) AS visits_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attr_receipts ELSE CAST(0 as FLOAT64) END) AS attr_receipts_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attr_gms ELSE CAST(0 as NUMERIC) END) AS attr_gms_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attr_rev_est ELSE CAST(0 as FLOAT64) END) AS attr_rev_dlly,
      sum(CASE WHEN c.flag = 'e' THEN c.impressions ELSE 0 END) AS impressions_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.clicks ELSE 0 END) AS clicks_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.cost ELSE CAST(0 as FLOAT64) END) AS cost_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.visits ELSE 0 END) AS visits_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attr_receipts ELSE CAST(0 as FLOAT64) END) AS attr_receipts_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attr_gms ELSE CAST(0 as NUMERIC) END) AS attr_gms_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attr_rev_est ELSE CAST(0 as FLOAT64) END) AS attr_rev_d3ly,
      sum(CASE WHEN c.flag = 'a' THEN c.insession_gms ELSE CAST(0 as NUMERIC) END) AS insession_gms,
      sum(CASE WHEN c.flag = 'a' THEN c.insession_orders ELSE 0 END) AS insession_orders,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_rev,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_receipts_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_gms ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_gms_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_new_gms_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_rev ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_rev_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_receipts_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_gms ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_gms_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_gms_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_rev_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_receipts_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_gms ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_gms_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_existing_gms_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_rev ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_rev_adjusted ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_adjusted,
      sum(CASE WHEN c.flag = 'a' THEN c.new_visits ELSE 0 END) AS new_visits,
      sum(CASE WHEN c.flag = 'a' THEN c.prolist_revenue ELSE CAST(0 as NUMERIC) END) AS prolist_revenue,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attr_receipts_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attr_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attr_gms_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_rev_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_new_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_lapsed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_purch_date,
      sum(CASE WHEN c.flag = 'a' THEN c.attributed_existing_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_purch_date,

      -- multiplier values
      sum(case when c.flag='a' then c.attributed_gms_mult else 0 end) as attributed_gms_mult,
      sum(case when c.flag='a' then c.attributed_attr_rev_mult else 0 end) as attributed_attr_rev_mult,
      sum(case when c.flag='a' then c.attributed_receipts_mult else 0 end) as attributed_receipts_mult,
      sum(case when c.flag='a' then c.attributed_new_receipts_mult else 0 end) as attributed_new_receipts_mult,
      sum(case when c.flag='a' then c.attributed_lapsed_receipts_mult else 0 end) as attributed_lapsed_receipts_mult,
      sum(case when c.flag='a' then c.attributed_existing_receipts_mult else 0 end) as attributed_existing_receipts_mult,
      sum(case when c.flag='a' then c.attributed_new_gms_mult else 0 end) as attributed_new_gms_mult,
      sum(case when c.flag='a' then c.attributed_lapsed_gms_mult else 0 end) as attributed_lapsed_gms_mult,
      sum(case when c.flag='a' then c.attributed_existing_gms_mult else 0 end) as attributed_existing_gms_mult,
      sum(case when c.flag='a' then c.attributed_new_attr_rev_mult else 0 end) as attributed_new_attr_rev_mult,
      sum(case when c.flag='a' then c.attributed_lapsed_attr_rev_mult else 0 end) as attributed_lapsed_attr_rev_mult,
      sum(case when c.flag='a' then c.attributed_existing_attr_rev_mult else 0 end) as attributed_existing_attr_rev_mult,
      sum(case when c.flag='a' then c.attributed_gms_adjusted_mult else 0 end) as attributed_gms_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_attr_rev_adjusted_mult else 0 end) as attributed_attr_rev_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_receipts_adjusted_mult else 0 end) as attributed_receipts_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_new_receipts_adjusted_mult else 0 end) as attributed_new_receipts_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_lapsed_receipts_adjusted_mult else 0 end) as attributed_lapsed_receipts_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_existing_receipts_adjusted_mult else 0 end) as attributed_existing_receipts_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_new_gms_adjusted_mult else 0 end) as attributed_new_gms_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_lapsed_gms_adjusted_mult else 0 end) as attributed_lapsed_gms_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_existing_gms_adjusted_mult else 0 end) as attributed_existing_gms_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_new_attr_rev_adjusted_mult else 0 end) as attributed_new_attr_rev_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_lapsed_attr_rev_adjusted_mult else 0 end) as attributed_lapsed_attr_rev_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_existing_attr_rev_adjusted_mult else 0 end) as attributed_existing_attr_rev_adjusted_mult,
      sum(case when c.flag='a' then c.attributed_gms_mult_fin else 0 end) as attributed_gms_mult_fin,
      sum(case when c.flag='a' then c.attributed_attr_rev_mult_fin else 0 end) as attributed_attr_rev_mult_fin,
      sum(case when c.flag='a' then c.attributed_receipts_mult_fin else 0 end) as attributed_receipts_mult_fin,
      sum(case when c.flag='a' then c.attributed_new_receipts_mult_fin else 0 end) as attributed_new_receipts_mult_fin,
      sum(case when c.flag='a' then c.attributed_lapsed_receipts_mult_fin else 0 end) as attributed_lapsed_receipts_mult_fin,
      sum(case when c.flag='a' then c.attributed_existing_receipts_mult_fin else 0 end) as attributed_existing_receipts_mult_fin,
      sum(case when c.flag='a' then c.attributed_new_gms_mult_fin else 0 end) as attributed_new_gms_mult_fin,
      sum(case when c.flag='a' then c.attributed_lapsed_gms_mult_fin else 0 end) as attributed_lapsed_gms_mult_fin,
      sum(case when c.flag='a' then c.attributed_existing_gms_mult_fin else 0 end) as attributed_existing_gms_mult_fin,
      sum(case when c.flag='a' then c.attributed_new_attr_rev_mult_fin else 0 end) as attributed_new_attr_rev_mult_fin,
      sum(case when c.flag='a' then c.attributed_lapsed_attr_rev_mult_fin else 0 end) as attributed_lapsed_attr_rev_mult_fin,
      sum(case when c.flag='a' then c.attributed_existing_attr_rev_mult_fin else 0 end) as attributed_existing_attr_rev_mult_fin,
      sum(case when c.flag='a' then c.attributed_gms_adjusted_mult_fin else 0 end) as attributed_gms_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_attr_rev_adjusted_mult_fin else 0 end) as attributed_attr_rev_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_receipts_adjusted_mult_fin else 0 end) as attributed_receipts_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_new_receipts_adjusted_mult_fin else 0 end) as attributed_new_receipts_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_lapsed_receipts_adjusted_mult_fin else 0 end) as attributed_lapsed_receipts_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_existing_receipts_adjusted_mult_fin else 0 end) as attributed_existing_receipts_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_new_gms_adjusted_mult_fin else 0 end) as attributed_new_gms_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_lapsed_gms_adjusted_mult_fin else 0 end) as attributed_lapsed_gms_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_existing_gms_adjusted_mult_fin else 0 end) as attributed_existing_gms_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_new_attr_rev_adjusted_mult_fin else 0 end) as attributed_new_attr_rev_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_lapsed_attr_rev_adjusted_mult_fin else 0 end) as attributed_lapsed_attr_rev_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_existing_attr_rev_adjusted_mult_fin else 0 end) as attributed_existing_attr_rev_adjusted_mult_fin,
      sum(case when c.flag='a' then c.attributed_gms_mult_purch_date else 0 end) as attributed_gms_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_attr_rev_mult_purch_date else 0 end) as attributed_attr_rev_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_receipts_mult_purch_date else 0 end) as attributed_receipts_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_new_receipts_mult_purch_date else 0 end) as attributed_new_receipts_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_lapsed_receipts_mult_purch_date else 0 end) as attributed_lapsed_receipts_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_existing_receipts_mult_purch_date else 0 end) as attributed_existing_receipts_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_new_gms_mult_purch_date else 0 end) as attributed_new_gms_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_lapsed_gms_mult_purch_date else 0 end) as attributed_lapsed_gms_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_existing_gms_mult_purch_date else 0 end) as attributed_existing_gms_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_new_attr_rev_mult_purch_date else 0 end) as attributed_new_attr_rev_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_lapsed_attr_rev_mult_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_existing_attr_rev_mult_purch_date else 0 end) as attributed_existing_attr_rev_mult_purch_date,
      sum(case when c.flag='a' then c.attributed_gms_mult_fin_purch_date else 0 end) as attributed_gms_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_attr_rev_mult_fin_purch_date else 0 end) as attributed_attr_rev_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_receipts_mult_fin_purch_date else 0 end) as attributed_receipts_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_new_receipts_mult_fin_purch_date else 0 end) as attributed_new_receipts_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_lapsed_receipts_mult_fin_purch_date else 0 end) as attributed_lapsed_receipts_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_existing_receipts_mult_fin_purch_date else 0 end) as attributed_existing_receipts_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_new_gms_mult_fin_purch_date else 0 end) as attributed_new_gms_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_lapsed_gms_mult_fin_purch_date else 0 end) as attributed_lapsed_gms_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_existing_gms_mult_fin_purch_date else 0 end) as attributed_existing_gms_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_new_attr_rev_mult_fin_purch_date else 0 end) as attributed_new_attr_rev_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_lapsed_attr_rev_mult_fin_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_fin_purch_date,
      sum(case when c.flag='a' then c.attributed_existing_attr_rev_mult_fin_purch_date else 0 end) as attributed_existing_attr_rev_mult_fin_purch_date,
      sum(case when c.flag='a' then c.gcp_costs_mult else 0 end) as gcp_costs_mult,
      sum(case when c.flag='a' then c.gcp_costs_mult_fin else 0 end) as gcp_costs_mult_fin,

      sum(CASE WHEN c.flag = 'b' THEN c.insession_gms ELSE CAST(0 as NUMERIC) END) AS insession_gms_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.insession_orders ELSE 0 END) AS insession_orders_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_rev_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_new_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_new_gms ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_new_rev ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_lapsed_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_lapsed_gms ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_lapsed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_existing_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_existing_gms ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_existing_rev ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.new_visits ELSE 0 END) AS new_visits_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.prolist_revenue ELSE CAST(0 as NUMERIC) END) AS prolist_revenue_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attr_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attr_receipts_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attr_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attr_gms_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_rev_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_new_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_new_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_new_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_lapsed_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_lapsed_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_lapsed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_existing_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_existing_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_purch_date_ly,
      sum(CASE WHEN c.flag = 'b' THEN c.attributed_existing_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_purch_date_ly,

      sum(case when c.flag='b' then c.attributed_gms_mult else 0 end) as attributed_gms_mult_ly,
      sum(case when c.flag='b' then c.attributed_attr_rev_mult else 0 end) as attributed_attr_rev_mult_ly,
      sum(case when c.flag='b' then c.attributed_receipts_mult else 0 end) as attributed_receipts_mult_ly,
      sum(case when c.flag='b' then c.attributed_new_receipts_mult else 0 end) as attributed_new_receipts_mult_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_receipts_mult else 0 end) as attributed_lapsed_receipts_mult_ly,
      sum(case when c.flag='b' then c.attributed_existing_receipts_mult else 0 end) as attributed_existing_receipts_mult_ly,
      sum(case when c.flag='b' then c.attributed_new_gms_mult else 0 end) as attributed_new_gms_mult_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_gms_mult else 0 end) as attributed_lapsed_gms_mult_ly,
      sum(case when c.flag='b' then c.attributed_existing_gms_mult else 0 end) as attributed_existing_gms_mult_ly,
      sum(case when c.flag='b' then c.attributed_new_attr_rev_mult else 0 end) as attributed_new_attr_rev_mult_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_attr_rev_mult else 0 end) as attributed_lapsed_attr_rev_mult_ly,
      sum(case when c.flag='b' then c.attributed_existing_attr_rev_mult else 0 end) as attributed_existing_attr_rev_mult_ly,
      sum(case when c.flag='b' then c.attributed_gms_mult_fin else 0 end) as attributed_gms_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_attr_rev_mult_fin else 0 end) as attributed_attr_rev_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_receipts_mult_fin else 0 end) as attributed_receipts_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_new_receipts_mult_fin else 0 end) as attributed_new_receipts_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_receipts_mult_fin else 0 end) as attributed_lapsed_receipts_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_existing_receipts_mult_fin else 0 end) as attributed_existing_receipts_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_new_gms_mult_fin else 0 end) as attributed_new_gms_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_gms_mult_fin else 0 end) as attributed_lapsed_gms_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_existing_gms_mult_fin else 0 end) as attributed_existing_gms_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_new_attr_rev_mult_fin else 0 end) as attributed_new_attr_rev_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_attr_rev_mult_fin else 0 end) as attributed_lapsed_attr_rev_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_existing_attr_rev_mult_fin else 0 end) as attributed_existing_attr_rev_mult_fin_ly,
      sum(case when c.flag='b' then c.attributed_gms_mult_purch_date else 0 end) as attributed_gms_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_attr_rev_mult_purch_date else 0 end) as attributed_attr_rev_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_receipts_mult_purch_date else 0 end) as attributed_receipts_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_new_receipts_mult_purch_date else 0 end) as attributed_new_receipts_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_receipts_mult_purch_date else 0 end) as attributed_lapsed_receipts_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_existing_receipts_mult_purch_date else 0 end) as attributed_existing_receipts_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_new_gms_mult_purch_date else 0 end) as attributed_new_gms_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_gms_mult_purch_date else 0 end) as attributed_lapsed_gms_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_existing_gms_mult_purch_date else 0 end) as attributed_existing_gms_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_new_attr_rev_mult_purch_date else 0 end) as attributed_new_attr_rev_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_attr_rev_mult_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_existing_attr_rev_mult_purch_date else 0 end) as attributed_existing_attr_rev_mult_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_gms_mult_fin_purch_date else 0 end) as attributed_gms_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_attr_rev_mult_fin_purch_date else 0 end) as attributed_attr_rev_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_receipts_mult_fin_purch_date else 0 end) as attributed_receipts_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_new_receipts_mult_fin_purch_date else 0 end) as attributed_new_receipts_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_receipts_mult_fin_purch_date else 0 end) as attributed_lapsed_receipts_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_existing_receipts_mult_fin_purch_date else 0 end) as attributed_existing_receipts_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_new_gms_mult_fin_purch_date else 0 end) as attributed_new_gms_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_gms_mult_fin_purch_date else 0 end) as attributed_lapsed_gms_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_existing_gms_mult_fin_purch_date else 0 end) as attributed_existing_gms_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_new_attr_rev_mult_fin_purch_date else 0 end) as attributed_new_attr_rev_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_lapsed_attr_rev_mult_fin_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.attributed_existing_attr_rev_mult_fin_purch_date else 0 end) as attributed_existing_attr_rev_mult_fin_purch_date_ly,
      sum(case when c.flag='b' then c.gcp_costs_mult else 0 end) as gcp_costs_mult_ly,
      sum(case when c.flag='b' then c.gcp_costs_mult_fin else 0 end) as gcp_costs_mult_fin_ly,

      sum(CASE WHEN c.flag = 'c' THEN c.insession_gms ELSE CAST(0 as NUMERIC) END) AS insession_gms_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.insession_orders ELSE 0 END) AS insession_orders_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_rev_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_new_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_new_gms ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_new_rev ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_lapsed_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_lapsed_gms ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_lapsed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_existing_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_existing_gms ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_existing_rev ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.new_visits ELSE 0 END) AS new_visits_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.prolist_revenue ELSE CAST(0 as NUMERIC) END) AS prolist_revenue_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attr_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attr_receipts_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attr_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attr_gms_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_rev_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_new_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_new_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_new_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_lapsed_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_lapsed_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_lapsed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_existing_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_existing_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_purch_date_dly,
      sum(CASE WHEN c.flag = 'c' THEN c.attributed_existing_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_purch_date_dly,

      sum(case when c.flag='c' then c.attributed_gms_mult else 0 end) as attributed_gms_mult_dly,
      sum(case when c.flag='c' then c.attributed_attr_rev_mult else 0 end) as attributed_attr_rev_mult_dly,
      sum(case when c.flag='c' then c.attributed_receipts_mult else 0 end) as attributed_receipts_mult_dly,
      sum(case when c.flag='c' then c.attributed_new_receipts_mult else 0 end) as attributed_new_receipts_mult_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_receipts_mult else 0 end) as attributed_lapsed_receipts_mult_dly,
      sum(case when c.flag='c' then c.attributed_existing_receipts_mult else 0 end) as attributed_existing_receipts_mult_dly,
      sum(case when c.flag='c' then c.attributed_new_gms_mult else 0 end) as attributed_new_gms_mult_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_gms_mult else 0 end) as attributed_lapsed_gms_mult_dly,
      sum(case when c.flag='c' then c.attributed_existing_gms_mult else 0 end) as attributed_existing_gms_mult_dly,
      sum(case when c.flag='c' then c.attributed_new_attr_rev_mult else 0 end) as attributed_new_attr_rev_mult_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_attr_rev_mult else 0 end) as attributed_lapsed_attr_rev_mult_dly,
      sum(case when c.flag='c' then c.attributed_existing_attr_rev_mult else 0 end) as attributed_existing_attr_rev_mult_dly,
      sum(case when c.flag='c' then c.attributed_gms_mult_fin else 0 end) as attributed_gms_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_attr_rev_mult_fin else 0 end) as attributed_attr_rev_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_receipts_mult_fin else 0 end) as attributed_receipts_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_new_receipts_mult_fin else 0 end) as attributed_new_receipts_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_receipts_mult_fin else 0 end) as attributed_lapsed_receipts_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_existing_receipts_mult_fin else 0 end) as attributed_existing_receipts_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_new_gms_mult_fin else 0 end) as attributed_new_gms_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_gms_mult_fin else 0 end) as attributed_lapsed_gms_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_existing_gms_mult_fin else 0 end) as attributed_existing_gms_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_new_attr_rev_mult_fin else 0 end) as attributed_new_attr_rev_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_attr_rev_mult_fin else 0 end) as attributed_lapsed_attr_rev_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_existing_attr_rev_mult_fin else 0 end) as attributed_existing_attr_rev_mult_fin_dly,
      sum(case when c.flag='c' then c.attributed_gms_mult_purch_date else 0 end) as attributed_gms_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_attr_rev_mult_purch_date else 0 end) as attributed_attr_rev_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_receipts_mult_purch_date else 0 end) as attributed_receipts_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_new_receipts_mult_purch_date else 0 end) as attributed_new_receipts_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_receipts_mult_purch_date else 0 end) as attributed_lapsed_receipts_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_existing_receipts_mult_purch_date else 0 end) as attributed_existing_receipts_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_new_gms_mult_purch_date else 0 end) as attributed_new_gms_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_gms_mult_purch_date else 0 end) as attributed_lapsed_gms_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_existing_gms_mult_purch_date else 0 end) as attributed_existing_gms_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_new_attr_rev_mult_purch_date else 0 end) as attributed_new_attr_rev_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_attr_rev_mult_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_existing_attr_rev_mult_purch_date else 0 end) as attributed_existing_attr_rev_mult_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_gms_mult_fin_purch_date else 0 end) as attributed_gms_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_attr_rev_mult_fin_purch_date else 0 end) as attributed_attr_rev_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_receipts_mult_fin_purch_date else 0 end) as attributed_receipts_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_new_receipts_mult_fin_purch_date else 0 end) as attributed_new_receipts_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_receipts_mult_fin_purch_date else 0 end) as attributed_lapsed_receipts_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_existing_receipts_mult_fin_purch_date else 0 end) as attributed_existing_receipts_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_new_gms_mult_fin_purch_date else 0 end) as attributed_new_gms_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_gms_mult_fin_purch_date else 0 end) as attributed_lapsed_gms_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_existing_gms_mult_fin_purch_date else 0 end) as attributed_existing_gms_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_new_attr_rev_mult_fin_purch_date else 0 end) as attributed_new_attr_rev_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_lapsed_attr_rev_mult_fin_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.attributed_existing_attr_rev_mult_fin_purch_date else 0 end) as attributed_existing_attr_rev_mult_fin_purch_date_dly,
      sum(case when c.flag='c' then c.gcp_costs_mult else 0 end) as gcp_costs_mult_dly,
      sum(case when c.flag='c' then c.gcp_costs_mult_fin else 0 end) as gcp_costs_mult_fin_dly,

      sum(CASE WHEN c.flag = 'd' THEN c.insession_gms ELSE CAST(0 as NUMERIC) END) AS insession_gms_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.insession_orders ELSE 0 END) AS insession_orders_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_rev_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_new_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_new_gms ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_new_rev ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_lapsed_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_lapsed_gms ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_lapsed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_existing_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_existing_gms ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_existing_rev ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.new_visits ELSE 0 END) AS new_visits_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.prolist_revenue ELSE CAST(0 as NUMERIC) END) AS prolist_revenue_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attr_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attr_receipts_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attr_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attr_gms_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_rev_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_new_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_new_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_new_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_lapsed_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_lapsed_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_lapsed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_existing_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_existing_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_purch_date_dlly,
      sum(CASE WHEN c.flag = 'd' THEN c.attributed_existing_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_purch_date_dlly,

      sum(case when c.flag='d' then c.attributed_gms_mult else 0 end) as attributed_gms_mult_dlly,
      sum(case when c.flag='d' then c.attributed_attr_rev_mult else 0 end) as attributed_attr_rev_mult_dlly,
      sum(case when c.flag='d' then c.attributed_receipts_mult else 0 end) as attributed_receipts_mult_dlly,
      sum(case when c.flag='d' then c.attributed_new_receipts_mult else 0 end) as attributed_new_receipts_mult_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_receipts_mult else 0 end) as attributed_lapsed_receipts_mult_dlly,
      sum(case when c.flag='d' then c.attributed_existing_receipts_mult else 0 end) as attributed_existing_receipts_mult_dlly,
      sum(case when c.flag='d' then c.attributed_new_gms_mult else 0 end) as attributed_new_gms_mult_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_gms_mult else 0 end) as attributed_lapsed_gms_mult_dlly,
      sum(case when c.flag='d' then c.attributed_existing_gms_mult else 0 end) as attributed_existing_gms_mult_dlly,
      sum(case when c.flag='d' then c.attributed_new_attr_rev_mult else 0 end) as attributed_new_attr_rev_mult_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_attr_rev_mult else 0 end) as attributed_lapsed_attr_rev_mult_dlly,
      sum(case when c.flag='d' then c.attributed_existing_attr_rev_mult else 0 end) as attributed_existing_attr_rev_mult_dlly,
      sum(case when c.flag='d' then c.attributed_gms_mult_fin else 0 end) as attributed_gms_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_attr_rev_mult_fin else 0 end) as attributed_attr_rev_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_receipts_mult_fin else 0 end) as attributed_receipts_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_new_receipts_mult_fin else 0 end) as attributed_new_receipts_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_receipts_mult_fin else 0 end) as attributed_lapsed_receipts_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_existing_receipts_mult_fin else 0 end) as attributed_existing_receipts_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_new_gms_mult_fin else 0 end) as attributed_new_gms_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_gms_mult_fin else 0 end) as attributed_lapsed_gms_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_existing_gms_mult_fin else 0 end) as attributed_existing_gms_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_new_attr_rev_mult_fin else 0 end) as attributed_new_attr_rev_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_attr_rev_mult_fin else 0 end) as attributed_lapsed_attr_rev_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_existing_attr_rev_mult_fin else 0 end) as attributed_existing_attr_rev_mult_fin_dlly,
      sum(case when c.flag='d' then c.attributed_gms_mult_purch_date else 0 end) as attributed_gms_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_attr_rev_mult_purch_date else 0 end) as attributed_attr_rev_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_receipts_mult_purch_date else 0 end) as attributed_receipts_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_new_receipts_mult_purch_date else 0 end) as attributed_new_receipts_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_receipts_mult_purch_date else 0 end) as attributed_lapsed_receipts_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_existing_receipts_mult_purch_date else 0 end) as attributed_existing_receipts_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_new_gms_mult_purch_date else 0 end) as attributed_new_gms_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_gms_mult_purch_date else 0 end) as attributed_lapsed_gms_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_existing_gms_mult_purch_date else 0 end) as attributed_existing_gms_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_new_attr_rev_mult_purch_date else 0 end) as attributed_new_attr_rev_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_attr_rev_mult_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_existing_attr_rev_mult_purch_date else 0 end) as attributed_existing_attr_rev_mult_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_gms_mult_fin_purch_date else 0 end) as attributed_gms_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_attr_rev_mult_fin_purch_date else 0 end) as attributed_attr_rev_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_receipts_mult_fin_purch_date else 0 end) as attributed_receipts_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_new_receipts_mult_fin_purch_date else 0 end) as attributed_new_receipts_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_receipts_mult_fin_purch_date else 0 end) as attributed_lapsed_receipts_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_existing_receipts_mult_fin_purch_date else 0 end) as attributed_existing_receipts_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_new_gms_mult_fin_purch_date else 0 end) as attributed_new_gms_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_gms_mult_fin_purch_date else 0 end) as attributed_lapsed_gms_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_existing_gms_mult_fin_purch_date else 0 end) as attributed_existing_gms_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_new_attr_rev_mult_fin_purch_date else 0 end) as attributed_new_attr_rev_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_lapsed_attr_rev_mult_fin_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.attributed_existing_attr_rev_mult_fin_purch_date else 0 end) as attributed_existing_attr_rev_mult_fin_purch_date_dlly,
      sum(case when c.flag='d' then c.gcp_costs_mult else 0 end) as gcp_costs_mult_dlly,
      sum(case when c.flag='d' then c.gcp_costs_mult_fin else 0 end) as gcp_costs_mult_fin_dlly,

      sum(CASE WHEN c.flag = 'e' THEN c.insession_gms ELSE CAST(0 as NUMERIC) END) AS insession_gms_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.insession_orders ELSE 0 END) AS insession_orders_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_rev_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_new_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_new_gms ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_new_rev ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_lapsed_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_lapsed_gms ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_lapsed_rev ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_existing_receipts ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_existing_gms ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_existing_rev ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.new_visits ELSE 0 END) AS new_visits_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.prolist_revenue ELSE CAST(0 as NUMERIC) END) AS prolist_revenue_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attr_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attr_receipts_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attr_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attr_gms_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_rev_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_new_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_receipts_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_new_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_new_gms_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_new_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_new_rev_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_lapsed_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_receipts_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_lapsed_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_lapsed_gms_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_lapsed_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_lapsed_rev_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_existing_receipts_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_receipts_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_existing_gms_purch_date ELSE CAST(0 as NUMERIC) END) AS attributed_existing_gms_purch_date_d3ly,
      sum(CASE WHEN c.flag = 'e' THEN c.attributed_existing_rev_purch_date ELSE CAST(0 as FLOAT64) END) AS attributed_existing_rev_purch_date_d3ly,

      sum(case when c.flag='e' then c.attributed_gms_mult else 0 end) as attributed_gms_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_attr_rev_mult else 0 end) as attributed_attr_rev_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_receipts_mult else 0 end) as attributed_receipts_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_new_receipts_mult else 0 end) as attributed_new_receipts_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_receipts_mult else 0 end) as attributed_lapsed_receipts_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_receipts_mult else 0 end) as attributed_existing_receipts_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_new_gms_mult else 0 end) as attributed_new_gms_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_gms_mult else 0 end) as attributed_lapsed_gms_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_gms_mult else 0 end) as attributed_existing_gms_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_new_attr_rev_mult else 0 end) as attributed_new_attr_rev_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_attr_rev_mult else 0 end) as attributed_lapsed_attr_rev_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_attr_rev_mult else 0 end) as attributed_existing_attr_rev_mult_d3ly,
      sum(case when c.flag='e' then c.attributed_gms_mult_fin else 0 end) as attributed_gms_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_attr_rev_mult_fin else 0 end) as attributed_attr_rev_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_receipts_mult_fin else 0 end) as attributed_receipts_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_new_receipts_mult_fin else 0 end) as attributed_new_receipts_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_receipts_mult_fin else 0 end) as attributed_lapsed_receipts_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_receipts_mult_fin else 0 end) as attributed_existing_receipts_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_new_gms_mult_fin else 0 end) as attributed_new_gms_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_gms_mult_fin else 0 end) as attributed_lapsed_gms_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_gms_mult_fin else 0 end) as attributed_existing_gms_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_new_attr_rev_mult_fin else 0 end) as attributed_new_attr_rev_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_attr_rev_mult_fin else 0 end) as attributed_lapsed_attr_rev_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_attr_rev_mult_fin else 0 end) as attributed_existing_attr_rev_mult_fin_d3ly,
      sum(case when c.flag='e' then c.attributed_gms_mult_purch_date else 0 end) as attributed_gms_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_attr_rev_mult_purch_date else 0 end) as attributed_attr_rev_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_receipts_mult_purch_date else 0 end) as attributed_receipts_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_new_receipts_mult_purch_date else 0 end) as attributed_new_receipts_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_receipts_mult_purch_date else 0 end) as attributed_lapsed_receipts_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_receipts_mult_purch_date else 0 end) as attributed_existing_receipts_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_new_gms_mult_purch_date else 0 end) as attributed_new_gms_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_gms_mult_purch_date else 0 end) as attributed_lapsed_gms_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_gms_mult_purch_date else 0 end) as attributed_existing_gms_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_new_attr_rev_mult_purch_date else 0 end) as attributed_new_attr_rev_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_attr_rev_mult_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_attr_rev_mult_purch_date else 0 end) as attributed_existing_attr_rev_mult_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_gms_mult_fin_purch_date else 0 end) as attributed_gms_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_attr_rev_mult_fin_purch_date else 0 end) as attributed_attr_rev_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_receipts_mult_fin_purch_date else 0 end) as attributed_receipts_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_new_receipts_mult_fin_purch_date else 0 end) as attributed_new_receipts_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_receipts_mult_fin_purch_date else 0 end) as attributed_lapsed_receipts_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_receipts_mult_fin_purch_date else 0 end) as attributed_existing_receipts_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_new_gms_mult_fin_purch_date else 0 end) as attributed_new_gms_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_gms_mult_fin_purch_date else 0 end) as attributed_lapsed_gms_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_gms_mult_fin_purch_date else 0 end) as attributed_existing_gms_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_new_attr_rev_mult_fin_purch_date else 0 end) as attributed_new_attr_rev_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_lapsed_attr_rev_mult_fin_purch_date else 0 end) as attributed_lapsed_attr_rev_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.attributed_existing_attr_rev_mult_fin_purch_date else 0 end) as attributed_existing_attr_rev_mult_fin_purch_date_d3ly,
      sum(case when c.flag='e' then c.gcp_costs_mult else 0 end) as gcp_costs_mult_d3ly,
      sum(case when c.flag='e' then c.gcp_costs_mult_fin else 0 end) as gcp_costs_mult_fin_d3ly,
    FROM
      c
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

--removing join to reduce error rate
/*
      LEFT OUTER JOIN performance_marketing_daily_tracker_temp AS b ON c.day = b.day
       AND c.account_name = b.account_name
       AND c.engine = b.engine
       AND c.campaign_id = CAST(b.campaign_id as STRING)
       AND c.campaign_name = b.campaign_name
       AND c.utm_medium = b.utm_medium
       AND c.utm_source = b.utm_source
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
*/

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.buyatt_rollups.performance_marketing_daily_tracker`
  AS (WITH unioned_historic_current as (SELECT *
      FROM `etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker_historical` WHERE DAY < '2020-03-01'
      UNION ALL
      SELECT * FROM performance_marketing_daily_tracker_final WHERE DAY >= '2020-03-01') 
    SELECT a.*, 
      case 
        when account_name like 'PLA%' then 'PLA'
        when account_name like 'SEM%' then 
          case when (account_name like '%non-Brand%' or campaign_name like '%nonbrand%' or account_name like '%NB%' or account_name like '%DSA%') then 'SEM - Non-Brand'
        else 'SEM - Brand' end 
      when lower(account_name) like 'youtube%' or account_name like 'DV360%' then 'Video'
      when account_name like '%Native Display%' then 'Display'
      when (account_name like 'Facebook%' and account_name not like '%Video%') then 'Paid Social'
      when (account_name like 'Facebook%' and (account_name like '%Video%' or lower(campaign_name) like '%psv%')) then 'Video'
      when account_name like 'Pinterest%' and lower(campaign_name) like '%video%' then 'Video'
      when account_name like 'Pinterest%' then 'Paid Social'
      when engine = 'affiliate' THEN 
          case when b.tactic = 'Social Creator Co' or b.publisher = 'CreatorIQ' then 'Social Creator Co'
          else 'Affiliates' end
      else 'Other Paid'
      end as reporting_channel_group,
      case 
        when account_name like '%Seller%' then 'Seller'
        when account_name like 'PLA%' then 'PLA'
        when engine = 'affiliate' then 'Affiliates'
        when (account_name like 'Facebook%' and account_name not like '%Video%') or (account_name like 'Pinterest%' and lower(campaign_name) not like '%video%') then 
          case when lower(campaign_name) like '%influencer%' or lower(campaign_name) like '%brand-connect%' then 'Influencer' 
          when (campaign_name like '%app install%' or campaign_name like '%skan%') then 'App'
          when campaign_name like '%seller%'then 'Seller'
          else 'Paid Social'
          end
        when account_name like 'SEM%' then 'SEM'
        when account_name like '%Native Display%' then 'Display'
        when (account_name like 'YouTube%' or account_name like 'DV360%' or account_name like '%Video%' or (account_name like 'Pinterest%' and lower(campaign_name) like '%video%')) then
          case when campaign_name like 'reserve_%' then 'ATL'
            when campaign_name like '%seller%' then 'Seller'
            when (lower(campaign_name) like '%midfunnel%' or lower(campaign_name) like '%psv%') then 'Midfunnel'
              else 'ATL Extension'
              end
        else 'Other'
      end as team,

      case 
        when account_name like '%Seller%' then 'Seller Acquisition/Retargeting'
        when ((account_name like 'Facebook%' or account_name like 'Pinterest%') and lower(campaign_name) like '%influencer%') then 'GMS-driving' 
        when (account_name like 'Facebook%' and (campaign_name like '%app install%' or campaign_name like '%skan%')) then 'App Install'
        when (account_name like 'Facebook%' and campaign_name like '%seller%') then 'Seller Acquisition/Retargeting' 
        when (account_name like '%Video%' or (account_name like 'Pinterest%' and lower(campaign_name) like '%video%')) and lower(campaign_name) like '%midfunnel%' then 'Midfunnel'
        else 'Revenue-driving'
      end as objective,

      case
        when account_name like 'PLA%' THEN
          case when campaign_name like '%ssc%' or campaign_name like '%[pm]%' or campaign_id = '14821205487' then 'PLA - Automatic'
          else 'PLA - Manual' end
        when account_name like 'SEM%' then 
          case when (account_name like '%non-Brand%' or campaign_name like '%nonbrand%'  or account_name like '%NB%'  or account_name like '%DSA%') then 'SEM - Non-Brand'
          else 'SEM - Brand' end 
        when engine = 'affiliate' THEN 
          case when b.publisher = 'CreatorIQ' then 'Social Creator Co - CreatorIQ'
          when b.tactic = 'Social Creator Co' and b.publisher != 'CreatorIQ' then 'Social Creator Co'
          else 'Affiliates' end  
        when (account_name like 'Facebook%' and account_name not like '%Video%') then 
          case when account_name like '%Dynamic%' then 'Paid Social - Dynamic'
          when account_name like '%Curated%' then 'Paid Social - Curated'
          when account_name like '% ASC %' then 'Paid Social - Optimized'
          else 'Paid Social - Other'
            end
        when (account_name like 'Pinterest%' and lower(campaign_name) not like '%video%') then 
          case when utm_medium like '%product%' then 'Paid Social - Dynamic'
          when utm_medium like '%curated%' then 'Paid Social - Curated'
          else 'Paid Social - Other'
            end
        when account_name like 'Native Display%' then 'Display - Native'
        when (account_name like 'YouTube%' or account_name like 'DV360%' or account_name like '%Video%' or (account_name like 'Pinterest%' and lower(campaign_name) not like '%video%')) then
          case when campaign_name like 'reserve_%' then 'Youtube - Reserved'
          else 'Digital Video - Programmatic' end
        else 'N/A'
      end as tactic_high_level,

      case 
        when account_name like '%Native Display%' then 
          case when account_name like '%bing%' then 'Display - MSAN'
          when (campaign_name like '%_gdn%' or campaign_name like 'gdn_%') then 'Display - GDN'
          when (campaign_name like '%_discovery%' or campaign_name like 'discovery_%') then 'Display - Discovery'
          else 'Display - Other' end
        when account_name like 'PLA%' then 
          case when campaign_name like '%_brand%' or campaign_id='12665398257' then 'PLA - Brand'
          when campaign_name like '%_max' or campaign_name like '%_ssc' or campaign_name like '%[pm]%' or campaign_id = '14821205487' then 'PLA - Smart Shopping'
          when lower(account_name) like '%megafeed%' then 'PLA - Megafeed'
          else 'PLA - Non-Brand' end
        when account_name like 'SEM%' then 
          case when (account_name like '%DSA%' or campaign_name like '%_dsa_%') then 'SEM - Non-Brand Dynamic'
          when (account_name like '%non-Brand%' or account_name like '%NB%' or campaign_name like '%nonbrand%') then 'SEM - Non-Brand Static'
          when (account_name like '%Branded%' or account_name like '%Brand%' or regexp_contains(campaign_name, r'_brand')) then 'SEM - Brand Static'
          else 'SEM - Other' end
        when account_name like 'Facebook%' then 
          case when campaign_name like '%_daba_%' then 'Facebook - DABA'
          when campaign_name like '%_dpa_%' then 'Facebook - DPA'
          when account_name like '%Video%' then 'Facebook - Video'
          when account_name like '%Curated%' then 'Facebook - Curated'
          when account_name like '% ASC %' then 'Facebook - ASC'
          else 'Facebook - Other'
          end
        when account_name like 'Pinterest%' then 
        case when lower(campaign_name) like '%video%' then 'Pinterest - Video'
           when (lower(campaign_name) like '%curated%' or lower(campaign_name) like '%collection%') then 'Pinterest - Collections'
           when (lower(campaign_name) like '% daba %' or lower(campaign_name) like '%shopping%') then 'Pinterest - Shopping'
          when lower(campaign_name) like '%_dpa_%' then 'Pinterest - DPA'
        else 'Pinterest - Other'
           end
        when (account_name like 'YouTube%' or account_name like 'DV360%' or account_name like '%Video%') then
          case when campaign_name like 'reserve_%' then 'Youtube - Reserved'
          when campaign_name like '%_psv_%' then 'Youtube - Programmatic'
          else 'Digital Video - Programmatic' end
        when account_name like '%Native Display%' then 'Display - Native'
        when engine = 'affiliate' THEN 
          case when b.publisher = 'CreatorIQ' then 'Social Creator Co - CreatorIQ'
          when b.tactic = 'Social Creator Co' and b.publisher != 'CreatorIQ' then 'Social Creator Co'
          else 'Affiliates' end  
        end as tactic_granular,
        case when (lower(campaign_name) like '%_rtg%' or lower(campaign_name) like '% rtg%') then 'Retargeting'
            when (lower(campaign_name) like '%_crm%' or lower(campaign_name) like '% crm%') then 'CRM'
          when (lower(campaign_name) like '%_pros%' or lower(campaign_name) like '% pros%') then 'Pros'
          else 'None/Other' end as audience
        FROM unioned_historic_current a
        left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` b on split(a.account_name,' ')[SAFE_OFFSET(0)] = b.publisher_id and a.engine = 'affiliate'
        ) ;

END;
