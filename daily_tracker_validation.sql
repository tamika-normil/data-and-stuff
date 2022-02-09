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
          WHEN upper(account) LIKE 'ETSY DISPLAY%' THEN concat('SEM Display ',coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],'Other'),' - google')
          WHEN upper(concat(coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(2)],''), ' ', coalesce(split(regexp_replace(account, '[^\\w]', '_'), '_')[SAFE_ORDINAL(3)],''))) LIKE 'DSA ' THEN 'SEM DSA US - google'
          WHEN upper(account) LIKE 'ETSY SEM US%BRANDED%' THEN 'SEM Brand US - google'
          WHEN upper(account) LIKE 'ETSY SEM US%NB%' THEN 'SEM non-Brand US - google'
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
          WHEN upper(account_name) LIKE 'ETSY DISPLAY%' THEN 'SEM Display US - bing'
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
            WHEN upper(account_name) LIKE '%BUYER ACQUISITION%' THEN 'Facebook - Other'
            ELSE CAST(NULL as STRING)
          END
          WHEN account_name like '%Global%' and campaign_name like '%Video%' then concat('Facebook Video - ',coalesce(trim(split(split(campaign_name,'|')[SAFE_ORDINAL(5)],'"')[SAFE_ORDINAL(1)]),'Other'))
          ELSE CASE
            WHEN upper(campaign_name) LIKE '%DABA%'
             OR upper(campaign_name) LIKE '%DPA%'
             OR upper(campaign_name) LIKE '%DYNAMIC%' THEN concat('Facebook', coalesce(split(coalesce(split(account_name, '-')[SAFE_ORDINAL(2)],''), '"')[SAFE_ORDINAL(1)],''), ' - Dynamic')
            WHEN upper(campaign_name) LIKE '%CURATED%' THEN concat('Facebook', coalesce(split(coalesce(split(account_name, '-')[SAFE_ORDINAL(2)],''), '"')[SAFE_ORDINAL(1)],''), ' - Curated')
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
        substr(CAST(publisher_id as STRING), 1, 80) AS account_name,
        sum(a.commission_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS cost,
        sum(a.sale_amount_amount * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) AS sales,
        0 AS impressions,
        'affiliate' AS engine
      FROM
        `etsy-data-warehouse-prod.marketing.awin_spend_data` AS a
        LEFT OUTER JOIN exchange AS b_0 ON a.commission_amount_currency = b_0.source_currency
         AND b_0.target_currency = 'USD'
         AND UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) BETWEEN b_0.create_date AND coalesce(b_0.cw_thru_date, UNIX_SECONDS(CAST(transaction_date AS TIMESTAMP)) )
      GROUP BY 1, 2 ),
 pinterest as (
       SELECT a.date AS day,
       'Pinterest' as account_name,
       CAMPAIGN_ID,
       CAMPAIGN_NAME,
       sum(SPEND_IN_DOLLAR * coalesce(b_0.market_rate / CAST(10000000 as BIGNUMERIC), CAST(1 as BIGNUMERIC))) as cost,
       sum(PIN_CLICK_PAID) as clicks,
       sum(IMPRESSION_PAID) as impressions,
       'pinterest' as engine
       FROM 
       -- source currency for nondomestic markets is EUR per CCA data team
       (select *, case when market = 'US' then 'USD' else 'EUR' end as source_currency from `etsy-data-warehouse-prod.marketing.pinterest_spend_daily`) a
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
        affiliate    )
   select * from all_markt);                                                                                              

#check daily differences
/*
with cost as 
    (SELECT date(day) as date, engine, sum(cost) cost
    #date_trunc(day, month) as month
    FROM all_markt
    group by 1,2),
channel_overview as
    (select  date(date) as date ,
    case when second_level_channel in ('gpla', 'google_ppc', 'intl_gpla', 'intl_ppc') then 'google'
    when second_level_channel in ('intl_bing_ppc', 'bing_ppc', 'bing_plas','intl_bing_plas') then 'bing'
    when second_level_channel in ('affiliates') then 'affiliate'
    when second_level_channel in ('facebook_disp', 'instagram_disp', 'facebook_disp_intl') then 'facebook'
    when second_level_channel in ('pinterest_disp') then 'pinterest'
    when second_level_channel in ('us_video','intl_video') then
        case when split(third_level_channel,'_')[SAFE_ORDINAL(1)]='facebook' then 'facebook'
        when split(third_level_channel,'_')[SAFE_ORDINAL(2)]='youtube' then 'google' end
    end as engine,
    #date_trunc(date, month) as month, 
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(coalesce(attributed_attr_rev_adjusted,0)) as rev,
    sum(coalesce(attributed_gms_ly,0)) as gms_ly,
    sum(coalesce(attributed_gms_dly,0)) as gms_dly,
    sum(coalesce(attributed_gms_dlly,0)) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview` a
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_level_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp')
    group by 1,2),
channel_overview_pd  as
    (select date(purchase_date) as date,
        case when second_channel in ('gpla', 'google_ppc', 'intl_gpla', 'intl_ppc') then 'google'
    when second_channel in ('intl_bing_ppc', 'bing_ppc', 'bing_plas','intl_bing_plas') then 'bing'
    when second_channel in ('affiliates') then 'affiliate'
    when second_channel in ('facebook_disp', 'instagram_disp', 'facebook_disp_intl') then 'facebook'
    when second_channel in ('pinterest_disp') then 'pinterest'
    when second_channel in ('us_video','intl_video') then
        case when split(third_channel,'_')[SAFE_ORDINAL(1)]='facebook' then 'facebook'
        when split(third_channel,'_')[SAFE_ORDINAL(2)]='youtube' then 'google' end
    end as engine,
    sum(coalesce(attributed_gms,0)) as gms_pd, 
    sum(coalesce(attributed_attr_rev,0)) as rev_pd,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date` a
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp')
    group by 1,2),
daily_tracker as 
    (SELECT date(day) as date, 
    engine,
    sum(cost) as cost, 
    sum(coalesce(attr_gms_est,0)) as gms, 
    sum(coalesce(attr_rev_est,0)) as rev,
    sum(attr_gms_purch_date) as gms_pd,
    sum(attributed_rev_purch_date) as rev_pd,
    sum(attr_gms_ly) as gms_ly,
    sum(attr_gms_dly) as gms_dly,
    sum(attr_gms_dlly) as gms_dlly,
    FROM  `etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker`
    #`etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker_historical`
    group by 1,2)   
select coalesce(coalesce(coalesce(a.date,b.date),c.date), d.date) as date, 
coalesce(coalesce(coalesce(a.engine,b.engine),c.engine), d.engine) as engine,
safe_divide((a.cost-b.cost),b.cost) as cost ,
safe_divide((a.gms-c.gms),c.gms) as gms,
safe_divide((a.rev-c.rev),c.rev) as rev,
safe_divide((a.gms_pd-d.gms_pd),d.gms_pd) as gms_pd,
safe_divide((a.rev_pd-d.rev_pd),d.rev_pd) as rev_pd,
safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from daily_tracker a
full outer join cost b using (date, engine)
full outer join channel_overview c using (date,engine)
full outer join channel_overview_pd d using (date,engine)
where date_trunc(coalesce(coalesce(coalesce(a.date,b.date),c.date), d.date), month) = '2022-02-01'
order by 1 desc; 
*/

#check historical differences

with cost as 
    (SELECT date_trunc(day, month) as month, engine,sum(coalesce(cost,0)) cost
    FROM all_markt
    group by 1,2),
channel_overview as
    (select date_trunc(date, month) as month,
      case when second_level_channel in ('gpla', 'google_ppc', 'intl_gpla', 'intl_ppc') then 'google'
    when second_level_channel in ('intl_bing_ppc', 'bing_ppc', 'bing_plas','intl_bing_plas') then 'bing'
    when second_level_channel in ('affiliates') then 'affiliate'
    when second_level_channel in ('facebook_disp', 'instagram_disp', 'facebook_disp_intl') then 'facebook'
    when second_level_channel in ('pinterest_disp') then 'pinterest'
    when second_level_channel in ('us_video','intl_video') then
        case when split(third_level_channel,'_')[SAFE_ORDINAL(1)]='facebook' then 'facebook'
        when split(third_level_channel,'_')[SAFE_ORDINAL(2)]='youtube' then 'google' end
    end as engine,
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(coalesce(attributed_attr_rev_adjusted,0)) as rev,
    sum(coalesce(attributed_gms_ly,0)) as gms_ly,
    sum(coalesce(attributed_gms_dly,0)) as gms_dly,
    sum(coalesce(attributed_gms_dlly,0)) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview`
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(second_level_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp')
    group by 1,2),
channel_overview_pd  as
    (select date_trunc(purchase_date, month) as month,
    case when second_channel in ('gpla', 'google_ppc', 'intl_gpla', 'intl_ppc') then 'google'
    when second_channel in ('intl_bing_ppc', 'bing_ppc', 'bing_plas','intl_bing_plas') then 'bing'
    when second_channel in ('affiliates') then 'affiliate'
    when second_channel in ('facebook_disp', 'instagram_disp', 'facebook_disp_intl') then 'facebook'
    when second_channel in ('pinterest_disp') then 'pinterest'
    when second_channel in ('us_video','intl_video') then
        case when split(third_channel,'_')[SAFE_ORDINAL(1)]='facebook' then 'facebook'
        when split(third_channel,'_')[SAFE_ORDINAL(2)]='youtube' then 'google' end
    end as engine,
    sum(coalesce(attributed_gms,0)) as gms_pd, 
    sum(coalesce(attributed_attr_rev,0)) as rev_pd,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date`
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(second_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp')
    group by 1,2),
daily_tracker as 
    (SELECT date(date_trunc(day, month)) as month, 
    engine,
    sum(coalesce(cost,0)) as cost, 
    sum(attr_gms_est) as gms, 
    sum(attr_rev_est) as rev,
    sum(attr_gms_purch_date) as gms_pd,
    sum(attributed_rev_purch_date) as rev_pd,
    sum(attr_gms_ly) as gms_ly,
    sum(attr_gms_dly) as gms_dly,
    sum(attr_gms_dlly) as gms_dlly,
    FROM  `etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker`
    #`etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker_historical`
    group by 1,2)   
select coalesce(coalesce(coalesce(a.month,b.month),c.month), d.month) as month,
 coalesce(coalesce(coalesce(a.engine,b.engine),c.engine), d.engine) as engine,
safe_divide((a.cost-b.cost),b.cost) as cost ,
safe_divide((a.gms-c.gms),c.gms) as gms,
safe_divide((a.rev-c.rev),c.rev) as rev,
safe_divide((a.gms_pd-d.gms_pd),d.gms_pd) as gms_pd,
safe_divide((a.rev_pd-d.rev_pd),d.rev_pd) as rev_pd,
safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from daily_tracker a
full outer join cost b using (month, engine)
full outer join channel_overview c using (month, engine)
full outer join channel_overview_pd d using (month,engine)
order by 1 desc; 


#check differences by publisher for affiliates
/*
with cost as 
    (SELECT date_trunc(day, month) as month, engine,account_name,sum(cost) cost
    FROM all_markt
    group by 1,2,3),
channel_overview as
    (select date_trunc(date, month) as month,
    utm_content as account_name,
      case when second_level_channel in ('gpla', 'google_ppc', 'intl_gpla', 'intl_ppc') then 'google'
    when second_level_channel in ('intl_bing_ppc', 'bing_ppc', 'bing_plas','intl_bing_plas') then 'bing'
    when second_level_channel in ('affiliates') then 'affiliate'
    when second_level_channel in ('facebook_disp', 'instagram_disp', 'facebook_disp_intl') then 'facebook'
    when second_level_channel in ('pinterest_disp') then 'pinterest'
    when second_level_channel in ('us_video','intl_video') then
        case when split(third_level_channel,'_')[SAFE_ORDINAL(1)]='facebook' then 'facebook'
        when split(third_level_channel,'_')[SAFE_ORDINAL(2)]='youtube' then 'google' end
    end as engine,
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(coalesce(attributed_attr_rev_adjusted,0)) as rev,
    sum(coalesce(attributed_gms_ly,0)) as gms_ly,
    sum(coalesce(attributed_gms_dly,0)) as gms_dly,
    sum(coalesce(attributed_gms_dlly,0)) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview`
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(second_level_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp')
    group by 1,2,3),
channel_overview_pd  as
    (select date_trunc(purchase_date, month) as month,
    utm_content as account_name,
    case when second_channel in ('gpla', 'google_ppc', 'intl_gpla', 'intl_ppc') then 'google'
    when second_channel in ('intl_bing_ppc', 'bing_ppc', 'bing_plas','intl_bing_plas') then 'bing'
    when second_channel in ('affiliates') then 'affiliate'
    when second_channel in ('facebook_disp', 'instagram_disp', 'facebook_disp_intl') then 'facebook'
    when second_channel in ('pinterest_disp') then 'pinterest'
    when second_channel in ('us_video','intl_video') then
        case when split(third_channel,'_')[SAFE_ORDINAL(1)]='facebook' then 'facebook'
        when split(third_channel,'_')[SAFE_ORDINAL(2)]='youtube' then 'google' end
    end as engine,
    sum(coalesce(attributed_gms,0)) as gms_pd, 
    sum(coalesce(attributed_attr_rev,0)) as rev_pd,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date`
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(second_channel) IN(
        'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp')
    group by 1,2,3),
daily_tracker as 
    (SELECT date(date_trunc(day, month)) as month, 
    engine,
    account_name,
    sum(cost) as cost, 
    sum(attr_gms_est) as gms, 
    sum(attr_rev_est) as rev,
    sum(attr_gms_purch_date) as gms_pd,
    sum(attributed_rev_purch_date) as rev_pd,
    sum(attr_gms_ly) as gms_ly,
    sum(attr_gms_dly) as gms_dly,
    sum(attr_gms_dlly) as gms_dlly,
    FROM  `etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker`
    #`etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker_historical`
    group by 1,2,3)   
select coalesce(coalesce(coalesce(a.month,b.month),c.month), d.month) as month,
 coalesce(coalesce(coalesce(a.engine,b.engine),c.engine), d.engine) as engine,
  coalesce(coalesce(coalesce(a.account_name,b.account_name),c.account_name), d.account_name) as engine,
safe_divide((a.cost-b.cost),b.cost) as cost ,
safe_divide((a.gms-c.gms),c.gms) as gms,
safe_divide((a.rev-c.rev),c.rev) as rev,
safe_divide((a.gms_pd-d.gms_pd),d.gms_pd) as gms_pd,
safe_divide((a.rev_pd-d.rev_pd),d.rev_pd) as rev_pd,
safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from daily_tracker a
full outer join cost b using (month, engine,account_name)
full outer join channel_overview c using (month, engine,account_name)
full outer join channel_overview_pd d using (month,engine,account_name)
where coalesce(coalesce(coalesce(a.month,b.month),c.month), d.month) = '2020-02-01'
and coalesce(coalesce(coalesce(a.engine,b.engine),c.engine), d.engine) = 'affiliate'
order by 1 desc;
*/ 

END
