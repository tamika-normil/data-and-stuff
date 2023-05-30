-- owner:  vbhuta@etsy.com, tnormil@etsy.com, performance-marketing@etsy.com, etsyanalytics@tinuiti.com
-- owner_team: marketinganalytics@etsy.com
-- description: rollup of visit and attributed performance for key channels, key regions, and split by device
begin

DECLARE top_landing_events ARRAY<STRING>;
-- Build an array of the top 100 landing events

SET top_landing_events = 
  (select ARRAY_AGG(landing_event)
  FROM ((
  select coalesce(substr(landing_event,1,35),'') as landing_event 
  from `etsy-data-warehouse-prod.buyatt_mart.visits_sum_by_date`
  group by 1
  ORDER BY sum(visits) DESC 
  LIMIT 100)
  union distinct
  select 'finds_page'
  union distinct
  select 'category_page'
  union distinct
  select 'pages_etsy_finds'
  union distinct
  select 'giftcard_index'
  union distinct
  select 'diy_view'
  union distinct
  select 'projects_hub'
  union distinct 
  select 'etsy_blog'
  union distinct 
  select 'discovery_feed'
  union distinct
  select 'browselistings'
  union distinct
  select 'homescreen' 
  union distinct
  select 'search_results' 
  union distinct
  select 'people_account' 
  union distinct 
  select 'search_shops'
  )
  );

drop table if exists `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`;
create or replace table `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
partition by `date`
cluster by device,marketing_region, key_market, channel_group as (                         
    select
        date(timestamp_seconds(a.run_date)) as `date`,  
        date_diff(current_date(),date(timestamp_seconds(a.run_date)),DAY)-1 as maturity,
        a.device,
        a.third_channel, 
        a.second_channel, 
        a.top_channel, 
        a.channel_group, 
        a.utm_campaign,
        a.utm_custom2,
        a.utm_medium,
        a.utm_source,
        a.utm_content,
        a.marketing_region,
        a.key_market, 
        a.landing_event,
        a.visit_market,
        coalesce(m.incrementality_multiplier_current,1) as incrementality_multiplier_current,
        coalesce(m.incrementality_multiplier_finance,1) as incrementality_multiplier_finance,
        FALSE as with_latency,
        a.visits as visits,
        a.insession_registrations,
        a.insession_converting_visits,
        a.insession_orders,
        a.insession_gms,
        a.new_visits,
        coalesce(a.gcp_costs,0) as gcp_costs, 
        coalesce(a.gcp_costs,0) * coalesce(m.incrementality_multiplier_current,1) as gcp_costs_mult,
        coalesce(a.gcp_costs,0) * coalesce(m.incrementality_multiplier_finance,1) as gcp_costs_mult_fin,
        f.insession_new_buyers,
        f.insession_new_buyer_gms,
        b.attributed_gms,
        coalesce(b.attributed_attr_rev,0)  + coalesce(d.spend,0) - coalesce(a.gcp_costs,0) as attributed_attr_rev,
        b.attributed_receipts,
        b.attributed_new_receipts,
        b.attributed_lapsed_receipts,
        b.attributed_existing_receipts,
        b.attributed_new_gms,
        b.attributed_lapsed_gms,
        b.attributed_existing_gms,
        b.attributed_new_attr_rev,
        b.attributed_lapsed_attr_rev,
        b.attributed_existing_attr_rev,
        coalesce(d.spend,0) as prolist_revenue,
        b.attributed_etsy_ads_revenue,
        b.attributed_etsy_ads_revenue_not_charged,
        -- adjusted metrics
        cast(b.attributed_gms as float64) as attributed_gms_adjusted,
        cast(coalesce(b.attributed_attr_rev,0) + coalesce(d.spend,0) - coalesce(a.gcp_costs,0) as float64) as attributed_attr_rev_adjusted,
        cast(b.attributed_receipts as float64) as attributed_receipts_adjusted,
        cast(b.attributed_new_receipts as float64) as attributed_new_receipts_adjusted,
        cast(b.attributed_lapsed_receipts as float64) as attributed_lapsed_receipts_adjusted,
        cast(b.attributed_existing_receipts as float64) as attributed_existing_receipts_adjusted,
        cast(b.attributed_new_gms as float64) as attributed_new_gms_adjusted,
        cast(b.attributed_lapsed_gms as float64) as attributed_lapsed_gms_adjusted,
        cast(b.attributed_existing_gms as float64) as attributed_existing_gms_adjusted,
        cast(b.attributed_new_attr_rev as float64) as attributed_new_attr_rev_adjusted,
        cast(b.attributed_lapsed_attr_rev as float64) as attributed_lapsed_attr_rev_adjusted,
        cast(b.attributed_existing_attr_rev as float64) as attributed_existing_attr_rev_adjusted
        -- metrics with current multiplier
        ,coalesce(b.attributed_gms,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_gms_mult
        ,coalesce(coalesce(b.attributed_attr_rev,0) + coalesce(d.spend,0),0)*coalesce(m.incrementality_multiplier_current,1) - (coalesce(a.gcp_costs,0)*coalesce(m.incrementality_multiplier_current,1)) as attributed_attr_rev_mult
        ,coalesce(b.attributed_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_receipts_mult
        ,coalesce(b.attributed_new_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_new_receipts_mult
        ,coalesce(b.attributed_lapsed_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_lapsed_receipts_mult
        ,coalesce(b.attributed_existing_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_existing_receipts_mult
        ,coalesce(b.attributed_new_gms,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_new_gms_mult
        ,coalesce(b.attributed_lapsed_gms,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_lapsed_gms_mult
        ,coalesce(b.attributed_existing_gms,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_existing_gms_mult
        ,coalesce(b.attributed_new_attr_rev,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_new_attr_rev_mult
        ,coalesce(b.attributed_lapsed_attr_rev,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_lapsed_attr_rev_mult
        ,coalesce(b.attributed_existing_attr_rev,0)*coalesce(m.incrementality_multiplier_current,1) as attributed_existing_attr_rev_mult
        -- adjusted metrics with current multiplier
        ,cast(coalesce(b.attributed_gms,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_gms_adjusted_mult
        ,cast(coalesce(coalesce(b.attributed_attr_rev,0) + coalesce(d.spend,0),0)*coalesce(m.incrementality_multiplier_current,1) - (coalesce(a.gcp_costs,0)*coalesce(m.incrementality_multiplier_current,1)) as float64) as attributed_attr_rev_adjusted_mult
        ,cast(coalesce(b.attributed_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_receipts_adjusted_mult
        ,cast(coalesce(b.attributed_new_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_new_receipts_adjusted_mult
        ,cast(coalesce(b.attributed_lapsed_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_lapsed_receipts_adjusted_mult
        ,cast(coalesce(b.attributed_existing_receipts,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_existing_receipts_adjusted_mult
        ,cast(coalesce(b.attributed_new_gms,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_new_gms_adjusted_mult
        ,cast(coalesce(b.attributed_lapsed_gms,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_lapsed_gms_adjusted_mult
        ,cast(coalesce(b.attributed_existing_gms,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_existing_gms_adjusted_mult
        ,cast(coalesce(b.attributed_new_attr_rev,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_new_attr_rev_adjusted_mult
        ,cast(coalesce(b.attributed_lapsed_attr_rev,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_lapsed_attr_rev_adjusted_mult
        ,cast(coalesce(b.attributed_existing_attr_rev,0)*coalesce(m.incrementality_multiplier_current,1) as float64) as attributed_existing_attr_rev_adjusted_mult
        -- metrics with finance multiplier
        ,coalesce(b.attributed_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_gms_mult_fin
        ,coalesce(coalesce(b.attributed_attr_rev,0) + coalesce(d.spend,0),0)*coalesce(m.incrementality_multiplier_finance,1) - (coalesce(a.gcp_costs,0)*coalesce(m.incrementality_multiplier_finance,1)) as attributed_attr_rev_mult_fin
        ,coalesce(b.attributed_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_receipts_mult_fin
        ,coalesce(b.attributed_new_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_new_receipts_mult_fin
        ,coalesce(b.attributed_lapsed_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_lapsed_receipts_mult_fin
        ,coalesce(b.attributed_existing_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_existing_receipts_mult_fin
        ,coalesce(b.attributed_new_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_new_gms_mult_fin
        ,coalesce(b.attributed_lapsed_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_lapsed_gms_mult_fin
        ,coalesce(b.attributed_existing_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_existing_gms_mult_fin
        ,coalesce(b.attributed_new_attr_rev,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_new_attr_rev_mult_fin
        ,coalesce(b.attributed_lapsed_attr_rev,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_lapsed_attr_rev_mult_fin
        ,coalesce(b.attributed_existing_attr_rev,0)*coalesce(m.incrementality_multiplier_finance,1) as attributed_existing_attr_rev_mult_fin
        -- adjusted metrics with finance multiplier
        ,cast(coalesce(b.attributed_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_gms_adjusted_mult_fin
        ,cast(coalesce(coalesce(b.attributed_attr_rev,0) + coalesce(d.spend,0),0)*coalesce(m.incrementality_multiplier_finance,1) - (coalesce(a.gcp_costs,0)*coalesce(m.incrementality_multiplier_finance,1))as float64) as attributed_attr_rev_adjusted_mult_fin
        ,cast(coalesce(b.attributed_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_receipts_adjusted_mult_fin
        ,cast(coalesce(b.attributed_new_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_new_receipts_adjusted_mult_fin
        ,cast(coalesce(b.attributed_lapsed_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_lapsed_receipts_adjusted_mult_fin
        ,cast(coalesce(b.attributed_existing_receipts,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_existing_receipts_adjusted_mult_fin
        ,cast(coalesce(b.attributed_new_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_new_gms_adjusted_mult_fin
        ,cast(coalesce(b.attributed_lapsed_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_lapsed_gms_adjusted_mult_fin
        ,cast(coalesce(b.attributed_existing_gms,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_existing_gms_adjusted_mult_fin
        ,cast(coalesce(b.attributed_new_attr_rev,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_new_attr_rev_adjusted_mult_fin
        ,cast(coalesce(b.attributed_lapsed_attr_rev,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_lapsed_attr_rev_adjusted_mult_fin
        ,cast(coalesce(b.attributed_existing_attr_rev,0)*coalesce(m.incrementality_multiplier_finance,1) as float64) as attributed_existing_attr_rev_adjusted_mult_fin
    from
        (select 
            run_date,  
            device,
            third_channel, 
            second_channel, 
            top_channel, 
            channel_group, 
            utm_campaign,
            utm_custom2,
            utm_medium,
            utm_source,
            utm_content,
            marketing_region,
            key_market, 
            case when coalesce(landing_event,'') not in unnest(top_landing_events) then 'other' 
                when coalesce(landing_event,'') = 'browselistings' then 'market'
                when coalesce(landing_event,'') = 'homescreen' then 'home'
                when coalesce(landing_event,'') = 'search_results' then 'search'
                when coalesce(landing_event,'') = 'people_account' then 'view_profile'
                else landing_event end as landing_event,
            visit_market,
            sum(visits) as visits,
            sum(case when second_channel in ('native_display') then 0 else visits * .0063 end) as gcp_costs,
            sum(insession_registrations) as insession_registrations,
            sum(insession_converting_visits) as insession_converting_visits,
            sum(insession_orders) as insession_orders,
            sum(insession_gms) as insession_gms,
            sum(new_visits) as new_visits,
        from `etsy-data-warehouse-prod.buyatt_mart.visits_sum_by_date`
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) a 
    left join 
        (select run_date, -- column name changed to date1 for partitioning
            device,
            third_channel, 
            second_channel, 
            top_channel, 
            channel_group, 
            utm_campaign,
            utm_custom2,
            utm_medium,
            utm_source,
            utm_content,
            marketing_region,
            key_market, 
            case when coalesce(landing_event,'') not in unnest(top_landing_events)
                then 'other' 
                when coalesce(landing_event,'') = 'browselistings' then 'market'
                when coalesce(landing_event,'') = 'homescreen' then 'home'
                when coalesce(landing_event,'') = 'search_results' then 'search'
                when coalesce(landing_event,'') = 'people_account' then 'view_profile'    
                else landing_event end as landing_event,
            visit_market,
            sum(insession_new_buyers) as insession_new_buyers,
            sum(insession_new_buyer_gms) as insession_new_buyer_gms,
        from `etsy-data-warehouse-prod.buyatt_mart.new_purch` 
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) f
    using (run_date,
        device,
        third_channel,
        second_channel,
        top_channel,
        channel_group,
        utm_campaign,
        utm_custom2,
        utm_medium,
        utm_source,
        utm_content,
        marketing_region,
        key_market,
        landing_event,
        visit_market)
    left join
        (select run_date,     
                device,
                third_channel,
                second_channel,
                top_channel,
                channel_group,
                utm_campaign,
                utm_custom2,
                utm_medium,
                utm_source,
                utm_content,
                marketing_region,
                key_market,
                case when coalesce(landing_event,'') not in unnest(top_landing_events)
                then 'other' 
                when coalesce(landing_event,'') = 'browselistings' then 'market'
                when coalesce(landing_event,'') = 'homescreen' then 'home'
                when coalesce(landing_event,'') = 'search_results' then 'search'
                when coalesce(landing_event,'') = 'people_account' then 'view_profile'    
                else landing_event end as landing_event,
                visit_market,
                sum(b.external_source_decay_all_gms) as attributed_gms,
                sum(b.external_source_decay_all_attr_rev) as attributed_attr_rev,
                sum(b.external_source_decay_all) as attributed_receipts,
                sum(b.external_source_decay_all*(cast(buyer_type= 'new' as int64))) as attributed_new_receipts,
                sum(b.external_source_decay_all*(cast(buyer_type= 'lapsed' as int64))) as attributed_lapsed_receipts,
                sum(b.external_source_decay_all*(cast(buyer_type= 'existing' as int64))) as attributed_existing_receipts,
                sum(b.external_source_decay_all_gms*(cast(buyer_type= 'new' as int64))) as attributed_new_gms,
                sum(b.external_source_decay_all_gms*(cast(buyer_type= 'lapsed' as int64))) as attributed_lapsed_gms,
                sum(b.external_source_decay_all_gms*(cast(buyer_type= 'existing' as int64))) as attributed_existing_gms,
                sum(b.external_source_decay_all_attr_rev*(cast(buyer_type= 'new' as int64))) as attributed_new_attr_rev,
                sum(b.external_source_decay_all_attr_rev*(cast(buyer_type= 'lapsed' as int64))) as attributed_lapsed_attr_rev,
                sum(b.external_source_decay_all_attr_rev*(cast(buyer_type= 'existing' as int64))) as attributed_existing_attr_rev,
                sum(b.external_source_decay_all_etsy_ads_revenue) as attributed_etsy_ads_revenue,
                sum(b.external_source_decay_all_etsy_ads_revenue_not_charged) as attributed_etsy_ads_revenue_not_charged
        from `etsy-data-warehouse-prod.buyatt_mart.attr_sum_visit_date` b
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) b
    using (run_date,   
        device,
        third_channel,
        second_channel,
        top_channel,
        channel_group,
        utm_campaign,
        utm_custom2,
        utm_medium,
        utm_source,
        utm_content,
        marketing_region,
        key_market,
        landing_event,
        visit_market)
    left join
        (select run_date,
            device,
            third_channel,
            second_channel,
            top_channel,
            channel_group,
            utm_campaign,
            utm_custom2,
            utm_medium,
            utm_source,
            utm_content,
            marketing_region,
            key_market,
            case when coalesce(landing_event,'') not in unnest(top_landing_events)
            then 'other' 
            when coalesce(landing_event,'') = 'browselistings' then 'market'
            when coalesce(landing_event,'') = 'homescreen' then 'home'
            when coalesce(landing_event,'') = 'search_results' then 'search'
            when coalesce(landing_event,'') = 'people_account' then 'view_profile'
            else landing_event end as landing_event,
            visit_market,
            sum(spend) as spend
        from
            `etsy-data-warehouse-prod.buyatt_mart.prolist_rollup` 
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) d
    using (run_date,
        device,
        third_channel,
        second_channel,
        top_channel,
        channel_group,
        utm_campaign,
        utm_custom2,
        utm_medium,
        utm_source,
        utm_content,
        marketing_region,
        key_market,
        landing_event,
        visit_market)
    left join (
        select 
            reporting_channel_group, 
            engine,
            tactic_high_level,
            tactic_granular,
            audience,
            top_channel,
            second_channel,
            third_channel,
            utm_campaign,
            utm_medium
        from `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions`) cd
    using(top_channel,second_channel,third_channel,utm_campaign,utm_medium)
    left join (
        select 
            date,
            incrementality_multiplier_current,
            incrementality_multiplier_finance,
            marketing_region,
            reporting_channel_group,
            engine,
            tactic_high_level,
            tactic_granular,
            audience
        from `etsy-data-warehouse-prod.buyatt_rollups.multiplier_log`) m
    on m.date = date(timestamp_seconds(a.run_date))
        and m.marketing_region = a.marketing_region
        and m.reporting_channel_group = cd.reporting_channel_group
        and m.engine = cd.engine
        and m.tactic_high_level = cd.tactic_high_level
        and m.tactic_granular = cd.tactic_granular
        and m.audience = cd.audience
);      


insert into `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
   select 
        h.`date`
        ,h.maturity
        ,h.device
        ,h.third_channel
        ,h.second_channel
        ,h.top_channel
        ,h.channel_group
        ,h.utm_campaign
        ,h.utm_custom2
        ,h.utm_medium
        ,h.utm_source
        ,h.utm_content
        ,h.marketing_region
        ,h.key_market
        ,h.landing_event
        ,h.visit_market
        ,h.incrementality_multiplier_current
        ,h.incrementality_multiplier_finance
        ,TRUE as with_latency
        ,visits
        ,insession_registrations
        ,insession_converting_visits
        ,insession_orders
        ,insession_gms
        ,new_visits
        ,gcp_costs
        ,gcp_costs_mult
        ,gcp_costs_mult_fin
        ,insession_new_buyers
        ,insession_new_buyer_gms
        ,h.attributed_gms
        ,attributed_attr_rev
        ,attributed_receipts
        ,attributed_new_receipts
        ,attributed_lapsed_receipts
        ,attributed_existing_receipts
        ,attributed_new_gms
        ,attributed_lapsed_gms
        ,attributed_existing_gms
        ,attributed_new_attr_rev
        ,attributed_lapsed_attr_rev
        ,attributed_existing_attr_rev
        ,prolist_revenue
        ,attributed_etsy_ads_revenue
        ,attributed_etsy_ads_revenue_not_charged
        --latency included metrics
        ,h.attributed_gms*a.final_index_weighted as attributed_gms_adjusted
        ,(coalesce(h.attributed_gms,0)*a.final_index_weighted*a.avg_rev_to_gms)+coalesce(h.prolist_revenue,0)-coalesce(h.gcp_costs,0) as attributed_attr_rev_adjusted
        ,h.attributed_receipts*a.final_index_weighted as attributed_receipts_adjusted
        ,h.attributed_new_receipts*a.final_index_weighted as attributed_new_receipts_adjusted
        ,h.attributed_lapsed_receipts*a.final_index_weighted as attributed_lapsed_receipts_adjusted
        ,h.attributed_existing_receipts*a.final_index_weighted as attributed_existing_receipts_adjusted
        ,h.attributed_new_gms*a.final_index_weighted as attributed_new_gms_adjusted
        ,h.attributed_lapsed_gms*a.final_index_weighted as attributed_lapsed_gms_adjusted
        ,h.attributed_existing_gms*a.final_index_weighted as attributed_existing_gms_adjusted
        ,h.attributed_new_gms*a.final_index_weighted*a.avg_rev_to_gms_new as attributed_new_attr_rev_adjusted
        ,h.attributed_lapsed_gms*a.final_index_weighted*a.avg_rev_to_gms_lapsed as attributed_lapsed_attr_rev_adjusted
        ,h.attributed_existing_gms*a.final_index_weighted*a.avg_rev_to_gms_existing as attributed_existing_attr_rev_adjusted
        -- metrics with current multiplier
        ,attributed_gms_mult
        ,attributed_attr_rev_mult
        ,attributed_receipts_mult
        ,attributed_new_receipts_mult
        ,attributed_lapsed_receipts_mult
        ,attributed_existing_receipts_mult
        ,attributed_new_gms_mult
        ,attributed_lapsed_gms_mult
        ,attributed_existing_gms_mult
        ,attributed_new_attr_rev_mult
        ,attributed_lapsed_attr_rev_mult
        ,attributed_existing_attr_rev_mult
        -- adjusted metrics with current multiplier
        ,coalesce((h.attributed_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_gms_adjusted_mult
        ,coalesce(((coalesce(h.attributed_gms,0)*a.final_index_weighted*a.avg_rev_to_gms)+coalesce(h.prolist_revenue,0)),0)*coalesce(h.incrementality_multiplier_current,1) -  
        (coalesce(h.gcp_costs,0)*coalesce(h.incrementality_multiplier_current,1)) as attributed_attr_rev_adjusted_mult
        ,coalesce((h.attributed_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_receipts_adjusted_mult
        ,coalesce((h.attributed_new_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_new_receipts_adjusted_mult
        ,coalesce((h.attributed_lapsed_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_lapsed_receipts_adjusted_mult
        ,coalesce((h.attributed_existing_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_existing_receipts_adjusted_mult
        ,coalesce((h.attributed_new_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_new_gms_adjusted_mult
        ,coalesce((h.attributed_lapsed_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_lapsed_gms_adjusted_mult
        ,coalesce((h.attributed_existing_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_existing_gms_adjusted_mult
        ,coalesce((h.attributed_new_gms*a.final_index_weighted*a.avg_rev_to_gms_new),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_new_attr_rev_adjusted_mult
        ,coalesce((h.attributed_lapsed_gms*a.final_index_weighted*a.avg_rev_to_gms_lapsed),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_lapsed_attr_rev_adjusted_mult
        ,coalesce((h.attributed_existing_gms*a.final_index_weighted*a.avg_rev_to_gms_existing),0)*coalesce(h.incrementality_multiplier_current,1) as attributed_existing_attr_rev_adjusted_mult
        -- metrics with finance multiplier
        ,attributed_gms_mult_fin
        ,attributed_attr_rev_mult_fin
        ,attributed_receipts_mult_fin
        ,attributed_new_receipts_mult_fin
        ,attributed_lapsed_receipts_mult_fin
        ,attributed_existing_receipts_mult_fin
        ,attributed_new_gms_mult_fin
        ,attributed_lapsed_gms_mult_fin
        ,attributed_existing_gms_mult_fin
        ,attributed_new_attr_rev_mult_fin
        ,attributed_lapsed_attr_rev_mult_fin
        ,attributed_existing_attr_rev_mult_fin
        -- adjusted metrics with finance multiplier
        ,coalesce((h.attributed_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_gms_adjusted_mult_fin
        ,coalesce(((coalesce(h.attributed_gms,0)*a.final_index_weighted*a.avg_rev_to_gms)+coalesce(h.prolist_revenue,0)),0)*coalesce(h.incrementality_multiplier_finance,1) 
         - (coalesce(h.gcp_costs,0)*coalesce(h.incrementality_multiplier_finance,1)) as attributed_attr_rev_adjusted_mult_fin
        ,coalesce((h.attributed_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_receipts_adjusted_mult_fin
        ,coalesce((h.attributed_new_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_new_receipts_adjusted_mult_fin
        ,coalesce((h.attributed_lapsed_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_lapsed_receipts_adjusted_mult_fin
        ,coalesce((h.attributed_existing_receipts*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_existing_receipts_adjusted_mult_fin
        ,coalesce((h.attributed_new_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_new_gms_adjusted_mult_fin
        ,coalesce((h.attributed_lapsed_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_lapsed_gms_adjusted_mult_fin
        ,coalesce((h.attributed_existing_gms*a.final_index_weighted),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_existing_gms_adjusted_mult_fin
        ,coalesce((h.attributed_new_gms*a.final_index_weighted*a.avg_rev_to_gms_new),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_new_attr_rev_adjusted_mult_fin
        ,coalesce((h.attributed_lapsed_gms*a.final_index_weighted*a.avg_rev_to_gms_lapsed),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_lapsed_attr_rev_adjusted_mult_fin
        ,coalesce((h.attributed_existing_gms*a.final_index_weighted*a.avg_rev_to_gms_existing),0)*coalesce(h.incrementality_multiplier_finance,1) as attributed_existing_attr_rev_adjusted_mult_fin
      from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` h
      left join `etsy-data-warehouse-prod.buyatt_rollups.latency_inputs` a 
        on a.channel_group = h.channel_group
        and a.marketing_region = case when h.marketing_region in ('US', 'GB', 'CA', 'AU', 'DE', 'FR') 
                                 then h.marketing_region else 'RoW' end
        and a.visit_date = h.`date`
        and a.run_date = current_date()
      where h.`date` > date_sub(current_date(), interval 31 day);


delete from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
where `date` > date_sub(current_date(), interval 31 day)
and not with_latency;


end;
