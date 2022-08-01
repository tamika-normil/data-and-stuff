#dev
BEGIN 

#check all channels
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
         AND commission_status in ('pending','approved')
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

with cost as 
    (SELECT date(day) as day,
    engine,
#cast(campaign_id as string) as campaign_id, 
sum(cost) cost
    FROM all_markt
    group by 1,2),
channel_overview as
    (select  date as day, 
    engine,
#utm_custom2 as campaign_id,
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(attributed_attr_rev_adjusted) as rev,
    #sum(attributed_gms_ly) as gms_ly,
    #sum(attributed_gms_dly) as gms_dly,
    #sum(attributed_gms_dlly) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
          left outer join `etsy-data-warehouse-dev.tnormil.performance_marketing_channels_def` b 
        on coalesce(a.utm_campaign,'') = coalesce(b.utm_campaign,'') and 
        coalesce(a.second_channel,'') = coalesce(b.second_channel,'') and 
        coalesce(a.third_channel,'') = coalesce(b.third_channel,'') and     
        coalesce(a.utm_medium,'') = coalesce(b.utm_medium,'') and 
        coalesce(a.utm_source,'') = coalesce(b.utm_source,'') and
        coalesce(a.landing_event,'') = coalesce(b.landing_event,'') and
        coalesce(a.utm_content,'') = coalesce(b.utm_content,'') and 
        coalesce(a.marketing_region,'') = coalesce(b.marketing_region,'') 
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN(    'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp', 'pinterest_disp_intl', 'native_display', 'intl_native_display')
    group by 1,2),
channel_overview_pd  as
    (select  purchase_date as day, 
    engine,
    sum(coalesce(attributed_gms,0)) as gms_pd, 
    sum(attributed_attr_rev) as rev_pd,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date` a
          left outer join `etsy-data-warehouse-dev.tnormil.performance_marketing_channels_def` b 
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
        ,'us_video','intl_video', 'pinterest_disp', 'pinterest_disp_intl', 'native_display', 'intl_native_display')
    group by 1,2),
daily_tracker as 
    (SELECT date(day) as day, 
    engine,
    sum(cost) as cost, 
    sum(coalesce(attr_gms_est,0)) as gms, 
    sum(attr_rev_est) as rev,
    sum(attr_gms_purch_date) as gms_pd,
    sum(attributed_rev_purch_date) as rev_pd,
    sum(attr_gms_ly) as gms_ly,
    sum(attr_gms_dly) as gms_dly,
    sum(attr_gms_dlly) as gms_dlly,
    #update table name here
    FROM etsy-data-warehouse-dev.tnormil.performance_marketing_daily_tracker
    group by 1,2)   
select coalesce(coalesce(coalesce(a.day,b.day),c.day), d.day) as day, 
coalesce(coalesce(coalesce(a.engine,b.engine),c.engine), engine) as engine, 
safe_divide((a.cost-b.cost),b.cost) as cost ,
safe_divide((a.gms-c.gms),c.gms) as gms,
safe_divide((a.rev-c.rev),c.rev) as rev,
safe_divide((a.gms_pd-d.gms_pd),d.gms_pd) as gms_pd,
safe_divide((a.rev_pd-d.rev_pd), d.rev_pd) as rev_pd,
#safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
#safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
#safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from daily_tracker a
full outer join cost b using (day, engine)
full outer join channel_overview c using (day, engine)
full outer join channel_overview_pd d using (day, engine)
where safe_divide((a.cost-b.cost),b.cost) is null
or abs(safe_divide((a.cost-b.cost),b.cost)) > .001
or safe_divide((a.gms-c.gms),c.gms) is null
or abs(safe_divide((a.gms-c.gms),c.gms)) > .001
or safe_divide((a.rev-c.rev),c.rev) is null
or abs(safe_divide((a.rev-c.rev),c.rev)) > .001
or safe_divide((a.gms_pd-d.gms_pd),d.gms_pd) is null
or abs(safe_divide((a.gms_pd-d.gms_pd),d.gms_pd)) > .001
or safe_divide((a.rev_pd-d.rev_pd),d.rev_pd) is null
or abs(safe_divide((a.rev_pd-d.rev_pd),d.rev_pd)) > .001
order by 2,1 desc;

END
