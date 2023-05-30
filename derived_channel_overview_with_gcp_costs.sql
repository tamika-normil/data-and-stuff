-- owner: marketinganalytics@etsy.com
-- revised for performance

 -- truncate table buyatt_rollups.derived_channel_overview_restricted;
--drop table if exists buyatt_rollups.derived_channel_overview_restricted;

begin 

create temp table keys
partition by `date`
cluster by device, marketing_region, key_market as (
 select distinct `date`,
        device
        ,marketing_region
        ,key_market 
        ,channel_group
        ,top_channel 
        ,second_channel
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
union distinct 
select distinct date_add(`date`, interval 1 year)  as `date`,
        device
        ,marketing_region 
        ,key_market 
        ,channel_group
        ,top_channel 
        ,second_channel 
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
where date_add(`date`, interval 1 year) < current_date
union distinct 
select distinct date_add(`date`, interval 52 week) as `date`,
        device
        ,marketing_region 
        ,key_market 
        ,channel_group
        ,top_channel 
        ,second_channel 
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
where date_add(`date`, interval 52 week) < current_date
union distinct 
select distinct date_add(`date`, interval 1 week) as `date`,
        device
        ,marketing_region 
        ,key_market as region
        ,channel_group
        ,top_channel 
        ,second_channel 
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
where date_add(`date`, interval 1 week) < current_date
union distinct 
select distinct date_add(`date`, interval 104 week) as `date`,
        device
        ,marketing_region 
        ,key_market as region
        ,channel_group
        ,top_channel 
        ,second_channel 
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
where date_add(`date`, interval 104 week) < current_date
union distinct 
select distinct date_add(`date`, interval 156 week) as `date`,
        device
        ,marketing_region 
        ,key_market as region
        ,channel_group
        ,top_channel 
        ,second_channel 
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
where date_add(`date`, interval 156 week) < current_date);


create or replace table `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted` as (
    select
        k.`date`
        ,k.device
        ,k.marketing_region as canonical_region
        ,k.key_market as region
        ,k.channel_group
        ,k.top_channel as top_level_channel
        ,k.second_channel as second_level_channel
        ,k.third_channel as third_level_channel
        ,k.utm_medium
        ,k.utm_source
        ,k.utm_campaign
        ,k.utm_custom2
        ,k.utm_content
        ,k.landing_event
        ,k.visit_market
        -- THIS YEAR
        ,a.incrementality_multiplier_current
        ,a.incrementality_multiplier_finance
        ,a.visits
        ,a.insession_registrations
        ,a.insession_converting_visits
        ,a.insession_orders
        ,a.insession_gms
        ,a.new_visits
        ,a.gcp_costs
        ,a.insession_new_buyers
        ,a.insession_new_buyer_gms
        ,a.attributed_gms
        ,a.attributed_attr_rev
        ,a.attributed_receipts
        ,a.attributed_new_receipts
        ,a.attributed_lapsed_receipts
        ,a.attributed_existing_receipts
        ,a.attributed_new_gms
        ,a.attributed_lapsed_gms
        ,a.attributed_existing_gms
        ,a.attributed_new_attr_rev
        ,a.attributed_lapsed_attr_rev
        ,a.attributed_existing_attr_rev
        ,a.prolist_revenue
        ,a.attributed_etsy_ads_revenue
        ,a.attributed_etsy_ads_revenue_not_charged
        ,a.attributed_gms_adjusted
        ,a.attributed_attr_rev_adjusted
        ,a.attributed_receipts_adjusted
        ,a.attributed_new_receipts_adjusted
        ,a.attributed_lapsed_receipts_adjusted
        ,a.attributed_existing_receipts_adjusted
        ,a.attributed_new_gms_adjusted
        ,a.attributed_lapsed_gms_adjusted
        ,a.attributed_existing_gms_adjusted
        ,a.attributed_new_attr_rev_adjusted
        ,a.attributed_lapsed_attr_rev_adjusted
        ,a.attributed_existing_attr_rev_adjusted
        -- -- metrics with current multiplier
        ,a.gcp_costs_mult 
        ,a.attributed_gms_mult
        ,a.attributed_attr_rev_mult
        ,a.attributed_receipts_mult
        ,a.attributed_new_receipts_mult
        ,a.attributed_lapsed_receipts_mult
        ,a.attributed_existing_receipts_mult
        ,a.attributed_new_gms_mult
        ,a.attributed_lapsed_gms_mult
        ,a.attributed_existing_gms_mult
        ,a.attributed_new_attr_rev_mult
        ,a.attributed_lapsed_attr_rev_mult
        ,a.attributed_existing_attr_rev_mult
        -- adjusted metrics with current multiplier
        ,a.attributed_gms_adjusted_mult
        ,a.attributed_attr_rev_adjusted_mult
        ,a.attributed_receipts_adjusted_mult
        ,a.attributed_new_receipts_adjusted_mult
        ,a.attributed_lapsed_receipts_adjusted_mult
        ,a.attributed_existing_receipts_adjusted_mult
        ,a.attributed_new_gms_adjusted_mult
        ,a.attributed_lapsed_gms_adjusted_mult
        ,a.attributed_existing_gms_adjusted_mult
        ,a.attributed_new_attr_rev_adjusted_mult
        ,a.attributed_lapsed_attr_rev_adjusted_mult
        ,a.attributed_existing_attr_rev_adjusted_mult
        -- metrics with finance multiplier
        ,a.gcp_costs_mult_fin
        ,a.attributed_gms_mult_fin
        ,a.attributed_attr_rev_mult_fin
        ,a.attributed_receipts_mult_fin
        ,a.attributed_new_receipts_mult_fin
        ,a.attributed_lapsed_receipts_mult_fin
        ,a.attributed_existing_receipts_mult_fin
        ,a.attributed_new_gms_mult_fin
        ,a.attributed_lapsed_gms_mult_fin
        ,a.attributed_existing_gms_mult_fin
        ,a.attributed_new_attr_rev_mult_fin
        ,a.attributed_lapsed_attr_rev_mult_fin
        ,a.attributed_existing_attr_rev_mult_fin
        -- adjusted metrics with finance multiplier
        ,a.attributed_gms_adjusted_mult_fin
        ,a.attributed_attr_rev_adjusted_mult_fin
        ,a.attributed_receipts_adjusted_mult_fin
        ,a.attributed_new_receipts_adjusted_mult_fin
        ,a.attributed_lapsed_receipts_adjusted_mult_fin
        ,a.attributed_existing_receipts_adjusted_mult_fin
        ,a.attributed_new_gms_adjusted_mult_fin
        ,a.attributed_lapsed_gms_adjusted_mult_fin
        ,a.attributed_existing_gms_adjusted_mult_fin
        ,a.attributed_new_attr_rev_adjusted_mult_fin
        ,a.attributed_lapsed_attr_rev_adjusted_mult_fin
        ,a.attributed_existing_attr_rev_adjusted_mult_fin
        
        -- LAST YEAR
        ,b.incrementality_multiplier_current as incrementality_multiplier_current_ly
        ,b.incrementality_multiplier_finance as incrementality_multiplier_finance_ly
        ,b.visits as visits_ly,
        b.insession_registrations as  insession_registrations_ly,
        b.insession_converting_visits as insession_converting_visits_ly,
        b.insession_orders as insession_orders_ly,
        b.insession_gms as insession_gms_ly,
        b.new_visits as new_visits_ly,
        b.gcp_costs as gcp_costs_ly,
        b.insession_new_buyers as insession_new_buyers_ly,
        b.insession_new_buyer_gms as insession_new_buyer_gms_ly,
        b.attributed_gms as attributed_gms_ly,
        b.attributed_attr_rev as attributed_attr_rev_ly,
        b.attributed_receipts as attributed_receipts_ly,
        b.attributed_new_receipts as attributed_new_receipts_ly,
        b.attributed_lapsed_receipts as  attributed_lapsed_receipts_ly,
        b.attributed_existing_receipts as  attributed_existing_receipts_ly,
        b.attributed_new_gms as  attributed_new_gms_ly,
        b.attributed_lapsed_gms as  attributed_lapsed_gms_ly,
        b.attributed_existing_gms as  attributed_existing_gms_ly,
        b.attributed_new_attr_rev as  attributed_new_attr_rev_ly,
        b.attributed_lapsed_attr_rev as  attributed_lapsed_attr_rev_ly,
        b.attributed_existing_attr_rev as  attributed_existing_attr_rev_ly,
        b.prolist_revenue as  prolist_revenue_ly,
        b.attributed_etsy_ads_revenue as  attributed_etsy_ads_revenue_ly,
        b.attributed_etsy_ads_revenue_not_charged as  attributed_etsy_ads_revenue_not_charged_ly,
        -- metrics with current multiplier
        b.gcp_costs_mult as gcp_costs_mult_ly,
        b.attributed_gms_mult as attributed_gms_mult_ly,
        b.attributed_attr_rev_mult as attributed_attr_rev_mult_ly,
        b.attributed_receipts_mult as attributed_receipts_mult_ly,
        b.attributed_new_receipts_mult as attributed_new_receipts_mult_ly,
        b.attributed_lapsed_receipts_mult as attributed_lapsed_receipts_mult_ly,
        b.attributed_existing_receipts_mult as attributed_existing_receipts_mult_ly,
        b.attributed_new_gms_mult as attributed_new_gms_mult_ly,
        b.attributed_lapsed_gms_mult as attributed_lapsed_gms_mult_ly,
        b.attributed_existing_gms_mult as attributed_existing_gms_mult_ly,
        b.attributed_new_attr_rev_mult as attributed_new_attr_rev_mult_ly,
        b.attributed_lapsed_attr_rev_mult as attributed_lapsed_attr_rev_mult_ly,
        b.attributed_existing_attr_rev_mult as attributed_existing_attr_rev_mult_ly,
        -- metrics with finance multiplier
        b.gcp_costs_mult_fin as gcp_costs_mult_fin_ly,
        b.attributed_gms_mult_fin as attributed_gms_mult_fin_ly,
        b.attributed_attr_rev_mult_fin as attributed_attr_rev_mult_fin_ly,
        b.attributed_receipts_mult_fin as attributed_receipts_mult_fin_ly,
        b.attributed_new_receipts_mult_fin as attributed_new_receipts_mult_fin_ly,
        b.attributed_lapsed_receipts_mult_fin as attributed_lapsed_receipts_mult_fin_ly,
        b.attributed_existing_receipts_mult_fin as attributed_existing_receipts_mult_fin_ly,
        b.attributed_new_gms_mult_fin as attributed_new_gms_mult_fin_ly,
        b.attributed_lapsed_gms_mult_fin as attributed_lapsed_gms_mult_fin_ly,
        b.attributed_existing_gms_mult_fin as attributed_existing_gms_mult_fin_ly,
        b.attributed_new_attr_rev_mult_fin as attributed_new_attr_rev_mult_fin_ly,
        b.attributed_lapsed_attr_rev_mult_fin as attributed_lapsed_attr_rev_mult_fin_ly,
        b.attributed_existing_attr_rev_mult_fin as attributed_existing_attr_rev_mult_fin_ly,

        -- LAST WEEK
        c.incrementality_multiplier_current as incrementality_multiplier_current_lw,
        c.incrementality_multiplier_finance as incrementality_multiplier_finance_lw,
        c.visits as visits_lw,
        c.insession_registrations as  insession_registrations_lw,
        c.insession_converting_visits as insession_converting_visits_lw,
        c.insession_orders as insession_orders_lw,
        c.insession_gms as insession_gms_lw,
        c.new_visits as new_visits_lw,
        c.gcp_costs as gcp_costs_lw,
        c.insession_new_buyers as insession_new_buyers_lw,
        c.insession_new_buyer_gms as insession_new_buyer_gms_lw,
        c.attributed_gms as attributed_gms_lw,
        c.attributed_attr_rev as attributed_attr_rev_lw,
        c.attributed_receipts as attributed_receipts_lw,
        c.attributed_new_receipts as attributed_new_receipts_lw,
        c.attributed_lapsed_receipts as  attributed_lapsed_receipts_lw,
        c.attributed_existing_receipts as  attributed_existing_receipts_lw,
        c.attributed_new_gms as  attributed_new_gms_lw,
        c.attributed_lapsed_gms as  attributed_lapsed_gms_lw,
        c.attributed_existing_gms as  attributed_existing_gms_lw,
        c.attributed_new_attr_rev as  attributed_new_attr_rev_lw,
        c.attributed_lapsed_attr_rev as  attributed_lapsed_attr_rev_lw,
        c.attributed_existing_attr_rev as  attributed_existing_attr_rev_lw,
        c.prolist_revenue as  prolist_revenue_lw,
        c.attributed_etsy_ads_revenue as  attributed_etsy_ads_revenue_lw,
        c.attributed_etsy_ads_revenue_not_charged as  attributed_etsy_ads_revenue_not_charged_lw,
        c.attributed_gms_adjusted as attributed_gms_adjusted_lw,
        c.attributed_attr_rev_adjusted as attributed_attr_rev_adjusted_lw,
        c.attributed_receipts_adjusted as attributed_receipts_adjusted_lw,
        c.attributed_new_receipts_adjusted as attributed_new_receipts_adjusted_lw,
        c.attributed_lapsed_receipts_adjusted as attributed_lapsed_receipts_adjusted_lw,
        c.attributed_existing_receipts_adjusted as attributed_existing_receipts_adjusted_lw,
        c.attributed_new_gms_adjusted as attributed_new_gms_adjusted_lw,
        c.attributed_lapsed_gms_adjusted as attributed_lapsed_gms_adjusted_lw,
        c.attributed_existing_gms_adjusted as attributed_existing_gms_adjusted_lw,
        c.attributed_new_attr_rev_adjusted as attributed_new_attr_rev_adjusted_lw,
        c.attributed_lapsed_attr_rev_adjusted as attributed_lapsed_attr_rev_adjusted_lw,
        c.attributed_existing_attr_rev_adjusted as attributed_existing_attr_rev_adjusted_lw,
        -- metrics with current multiplier
        c.gcp_costs_mult as gcp_costs_mult_lw,
        c.attributed_gms_mult as attributed_gms_mult_lw,
        c.attributed_attr_rev_mult as attributed_attr_rev_mult_lw,
        c.attributed_receipts_mult as attributed_receipts_mult_lw,
        c.attributed_new_receipts_mult as attributed_new_receipts_mult_lw,
        c.attributed_lapsed_receipts_mult as attributed_lapsed_receipts_mult_lw,
        c.attributed_existing_receipts_mult as attributed_existing_receipts_mult_lw,
        c.attributed_new_gms_mult as attributed_new_gms_mult_lw,
        c.attributed_lapsed_gms_mult as attributed_lapsed_gms_mult_lw,
        c.attributed_existing_gms_mult as attributed_existing_gms_mult_lw,
        c.attributed_new_attr_rev_mult as attributed_new_attr_rev_mult_lw,
        c.attributed_lapsed_attr_rev_mult as attributed_lapsed_attr_rev_mult_lw,
        c.attributed_existing_attr_rev_mult as attributed_existing_attr_rev_mult_lw,
        -- adjusted metrics with current multiplier
        c.attributed_gms_adjusted_mult as attributed_gms_adjusted_mult_lw,
        c.attributed_attr_rev_adjusted_mult as attributed_attr_rev_adjusted_mult_lw,
        c.attributed_receipts_adjusted_mult as attributed_receipts_adjusted_mult_lw,
        c.attributed_new_receipts_adjusted_mult as attributed_new_receipts_adjusted_mult_lw,
        c.attributed_lapsed_receipts_adjusted_mult as attributed_lapsed_receipts_adjusted_mult_lw,
        c.attributed_existing_receipts_adjusted_mult as attributed_existing_receipts_adjusted_mult_lw,
        c.attributed_new_gms_adjusted_mult as attributed_new_gms_adjusted_mult_lw,
        c.attributed_lapsed_gms_adjusted_mult as attributed_lapsed_gms_adjusted_mult_lw,
        c.attributed_existing_gms_adjusted_mult as attributed_existing_gms_adjusted_mult_lw,
        c.attributed_new_attr_rev_adjusted_mult as attributed_new_attr_rev_adjusted_mult_lw,
        c.attributed_lapsed_attr_rev_adjusted_mult as attributed_lapsed_attr_rev_adjusted_mult_lw,
        c.attributed_existing_attr_rev_adjusted_mult as attributed_existing_attr_rev_adjusted_mult_lw,
        -- metrics with finance multiplier
        c.gcp_costs_mult_fin as gcp_costs_mult_fin_lw,
        c.attributed_gms_mult_fin as attributed_gms_mult_fin_lw,
        c.attributed_attr_rev_mult_fin as attributed_attr_rev_mult_fin_lw,
        c.attributed_receipts_mult_fin as attributed_receipts_mult_fin_lw,
        c.attributed_new_receipts_mult_fin as attributed_new_receipts_mult_fin_lw,
        c.attributed_lapsed_receipts_mult_fin as attributed_lapsed_receipts_mult_fin_lw,
        c.attributed_existing_receipts_mult_fin as attributed_existing_receipts_mult_fin_lw,
        c.attributed_new_gms_mult_fin as attributed_new_gms_mult_fin_lw,
        c.attributed_lapsed_gms_mult_fin as attributed_lapsed_gms_mult_fin_lw,
        c.attributed_existing_gms_mult_fin as attributed_existing_gms_mult_fin_lw,
        c.attributed_new_attr_rev_mult_fin as attributed_new_attr_rev_mult_fin_lw,
        c.attributed_lapsed_attr_rev_mult_fin as attributed_lapsed_attr_rev_mult_fin_lw,
        c.attributed_existing_attr_rev_mult_fin as attributed_existing_attr_rev_mult_fin_lw,
        -- adjusted metrics with finance multiplier
        c.attributed_gms_adjusted_mult_fin as attributed_gms_adjusted_mult_fin_lw,
        c.attributed_attr_rev_adjusted_mult_fin as attributed_attr_rev_adjusted_mult_fin_lw,
        c.attributed_receipts_adjusted_mult_fin as attributed_receipts_adjusted_mult_fin_lw,
        c.attributed_new_receipts_adjusted_mult_fin as attributed_new_receipts_adjusted_mult_fin_lw,
        c.attributed_lapsed_receipts_adjusted_mult_fin as attributed_lapsed_receipts_adjusted_mult_fin_lw,
        c.attributed_existing_receipts_adjusted_mult_fin as attributed_existing_receipts_adjusted_mult_fin_lw,
        c.attributed_new_gms_adjusted_mult_fin as attributed_new_gms_adjusted_mult_fin_lw,
        c.attributed_lapsed_gms_adjusted_mult_fin as attributed_lapsed_gms_adjusted_mult_fin_lw,
        c.attributed_existing_gms_adjusted_mult_fin as attributed_existing_gms_adjusted_mult_fin_lw,
        c.attributed_new_attr_rev_adjusted_mult_fin as attributed_new_attr_rev_adjusted_mult_fin_lw,
        c.attributed_lapsed_attr_rev_adjusted_mult_fin as attributed_lapsed_attr_rev_adjusted_mult_fin_lw,
        c.attributed_existing_attr_rev_adjusted_mult_fin as attributed_existing_attr_rev_adjusted_mult_fin_lw,

        -- SAME DAY LAST YEAR
        d.incrementality_multiplier_current as incrementality_multiplier_current_dly,
        d.incrementality_multiplier_finance as incrementality_multiplier_finance_dly,
        d.visits as visits_dly,
        d.insession_registrations as  insession_registrations_dly,
        d.insession_converting_visits as insession_converting_visits_dly,
        d.insession_orders as insession_orders_dly,
        d.insession_gms as insession_gms_dly,
        d.new_visits as new_visits_dly,
        d.gcp_costs as gcp_costs_dly,
        d.insession_new_buyers as insession_new_buyers_dly,
        d.insession_new_buyer_gms as insession_new_buyer_gms_dly,
        d.attributed_gms as attributed_gms_dly,
        d.attributed_attr_rev as attributed_attr_rev_dly,
        d.attributed_receipts as attributed_receipts_dly,
        d.attributed_new_receipts as attributed_new_receipts_dly,
        d.attributed_lapsed_receipts as  attributed_lapsed_receipts_dly,
        d.attributed_existing_receipts as  attributed_existing_receipts_dly,
        d.attributed_new_gms as  attributed_new_gms_dly,
        d.attributed_lapsed_gms as  attributed_lapsed_gms_dly,
        d.attributed_existing_gms as  attributed_existing_gms_dly,
        d.attributed_new_attr_rev as  attributed_new_attr_rev_dly,
        d.attributed_lapsed_attr_rev as  attributed_lapsed_attr_rev_dly,
        d.attributed_existing_attr_rev as  attributed_existing_attr_rev_dly,
        d.prolist_revenue as  prolist_revenue_dly,
        d.attributed_etsy_ads_revenue as  attributed_etsy_ads_revenue_dly,
        d.attributed_etsy_ads_revenue_not_charged as  attributed_etsy_ads_revenue_not_charged_dly,
        -- metrics with current multiplier
        d.gcp_costs_mult as gcp_costs_mult_dly,
        d.attributed_gms_mult as attributed_gms_mult_dly,
        d.attributed_attr_rev_mult as attributed_attr_rev_mult_dly,
        d.attributed_receipts_mult as attributed_receipts_mult_dly,
        d.attributed_new_receipts_mult as attributed_new_receipts_mult_dly,
        d.attributed_lapsed_receipts_mult as attributed_lapsed_receipts_mult_dly,
        d.attributed_existing_receipts_mult as attributed_existing_receipts_mult_dly,
        d.attributed_new_gms_mult as attributed_new_gms_mult_dly,
        d.attributed_lapsed_gms_mult as attributed_lapsed_gms_mult_dly,
        d.attributed_existing_gms_mult as attributed_existing_gms_mult_dly,
        d.attributed_new_attr_rev_mult as attributed_new_attr_rev_mult_dly,
        d.attributed_lapsed_attr_rev_mult as attributed_lapsed_attr_rev_mult_dly,
        d.attributed_existing_attr_rev_mult as attributed_existing_attr_rev_mult_dly,
        -- metrics with finance multiplier
        d.gcp_costs_mult_fin as gcp_costs_mult_fin_dly,
        d.attributed_gms_mult_fin as attributed_gms_mult_fin_dly,
        d.attributed_attr_rev_mult_fin as attributed_attr_rev_mult_fin_dly,
        d.attributed_receipts_mult_fin as attributed_receipts_mult_fin_dly,
        d.attributed_new_receipts_mult_fin as attributed_new_receipts_mult_fin_dly,
        d.attributed_lapsed_receipts_mult_fin as attributed_lapsed_receipts_mult_fin_dly,
        d.attributed_existing_receipts_mult_fin as attributed_existing_receipts_mult_fin_dly,
        d.attributed_new_gms_mult_fin as attributed_new_gms_mult_fin_dly,
        d.attributed_lapsed_gms_mult_fin as attributed_lapsed_gms_mult_fin_dly,
        d.attributed_existing_gms_mult_fin as attributed_existing_gms_mult_fin_dly,
        d.attributed_new_attr_rev_mult_fin as attributed_new_attr_rev_mult_fin_dly,
        d.attributed_lapsed_attr_rev_mult_fin as attributed_lapsed_attr_rev_mult_fin_dly,
        d.attributed_existing_attr_rev_mult_fin as attributed_existing_attr_rev_mult_fin_dly,

        -- SAME DAY 2 YEARS AGO
        e.incrementality_multiplier_current as incrementality_multiplier_current_dlly,
        e.incrementality_multiplier_finance as incrementality_multiplier_finance_dlly,
        e.visits as visits_dlly,
        e.insession_registrations as  insession_registrations_dlly,
        e.insession_converting_visits as insession_converting_visits_dlly,
        e.insession_orders as insession_orders_dlly,
        e.insession_gms as insession_gms_dlly,
        e.new_visits as new_visits_dlly,
        e.gcp_costs as gcp_costs_dlly, 
        e.insession_new_buyers as insession_new_buyers_dlly,
        e.insession_new_buyer_gms as insession_new_buyer_gms_dlly,
        e.attributed_gms as attributed_gms_dlly,
        e.attributed_attr_rev as attributed_attr_rev_dlly,
        e.attributed_receipts as attributed_receipts_dlly,
        e.attributed_new_receipts as attributed_new_receipts_dlly,
        e.attributed_lapsed_receipts as  attributed_lapsed_receipts_dlly,
        e.attributed_existing_receipts as  attributed_existing_receipts_dlly,
        e.attributed_new_gms as  attributed_new_gms_dlly,
        e.attributed_lapsed_gms as  attributed_lapsed_gms_dlly,
        e.attributed_existing_gms as  attributed_existing_gms_dlly,
        e.attributed_new_attr_rev as  attributed_new_attr_rev_dlly,
        e.attributed_lapsed_attr_rev as  attributed_lapsed_attr_rev_dlly,
        e.attributed_existing_attr_rev as  attributed_existing_attr_rev_dlly,
        e.prolist_revenue as  prolist_revenue_dlly,
        e.attributed_etsy_ads_revenue as  attributed_etsy_ads_revenue_dlly,
        e.attributed_etsy_ads_revenue_not_charged as  attributed_etsy_ads_revenue_not_charged_dlly,
        -- metrics with current multiplier
        e.gcp_costs_mult as gcp_costs_mult_dlly, 
        e.attributed_gms_mult as attributed_gms_mult_dlly,
        e.attributed_attr_rev_mult as attributed_attr_rev_mult_dlly,
        e.attributed_receipts_mult as attributed_receipts_mult_dlly,
        e.attributed_new_receipts_mult as attributed_new_receipts_mult_dlly,
        e.attributed_lapsed_receipts_mult as attributed_lapsed_receipts_mult_dlly,
        e.attributed_existing_receipts_mult as attributed_existing_receipts_mult_dlly,
        e.attributed_new_gms_mult as attributed_new_gms_mult_dlly,
        e.attributed_lapsed_gms_mult as attributed_lapsed_gms_mult_dlly,
        e.attributed_existing_gms_mult as attributed_existing_gms_mult_dlly,
        e.attributed_new_attr_rev_mult as attributed_new_attr_rev_mult_dlly,
        e.attributed_lapsed_attr_rev_mult as attributed_lapsed_attr_rev_mult_dlly,
        e.attributed_existing_attr_rev_mult as attributed_existing_attr_rev_mult_dlly,
        -- metrics with finance multiplier
        e.gcp_costs_mult_fin as gcp_costs_mult_fin_dlly, 
        e.attributed_gms_mult_fin as attributed_gms_mult_fin_dlly,
        e.attributed_attr_rev_mult_fin as attributed_attr_rev_mult_fin_dlly,
        e.attributed_receipts_mult_fin as attributed_receipts_mult_fin_dlly,
        e.attributed_new_receipts_mult_fin as attributed_new_receipts_mult_fin_dlly,
        e.attributed_lapsed_receipts_mult_fin as attributed_lapsed_receipts_mult_fin_dlly,
        e.attributed_existing_receipts_mult_fin as attributed_existing_receipts_mult_fin_dlly,
        e.attributed_new_gms_mult_fin as attributed_new_gms_mult_fin_dlly,
        e.attributed_lapsed_gms_mult_fin as attributed_lapsed_gms_mult_fin_dlly,
        e.attributed_existing_gms_mult_fin as attributed_existing_gms_mult_fin_dlly,
        e.attributed_new_attr_rev_mult_fin as attributed_new_attr_rev_mult_fin_dlly,
        e.attributed_lapsed_attr_rev_mult_fin as attributed_lapsed_attr_rev_mult_fin_dlly,
        e.attributed_existing_attr_rev_mult_fin as attributed_existing_attr_rev_mult_fin_dlly,

        -- SAME DAY 3 YEARS AGO
        f.incrementality_multiplier_current as incrementality_multiplier_current_d3ly,
        f.incrementality_multiplier_finance as incrementality_multiplier_finance_d3ly,
        f.visits as visits_d3ly,
        f.insession_registrations as  insession_registrations_d3ly,
        f.insession_converting_visits as insession_converting_visits_d3ly,
        f.insession_orders as insession_orders_d3ly,
        f.insession_gms as insession_gms_d3ly,
        f.new_visits as new_visits_d3ly,
        f.gcp_costs as gcp_costs_d3ly, 
        f.insession_new_buyers as insession_new_buyers_d3ly,
        f.insession_new_buyer_gms as insession_new_buyer_gms_d3ly,
        f.attributed_gms as attributed_gms_d3ly,
        f.attributed_attr_rev as attributed_attr_rev_d3ly,
        f.attributed_receipts as attributed_receipts_d3ly,
        f.attributed_new_receipts as attributed_new_receipts_d3ly,
        f.attributed_lapsed_receipts as  attributed_lapsed_receipts_d3ly,
        f.attributed_existing_receipts as  attributed_existing_receipts_d3ly,
        f.attributed_new_gms as  attributed_new_gms_d3ly,
        f.attributed_lapsed_gms as  attributed_lapsed_gms_d3ly,
        f.attributed_existing_gms as  attributed_existing_gms_d3ly,
        f.attributed_new_attr_rev as  attributed_new_attr_rev_d3ly,
        f.attributed_lapsed_attr_rev as  attributed_lapsed_attr_rev_d3ly,
        f.attributed_existing_attr_rev as  attributed_existing_attr_rev_d3ly,
        f.prolist_revenue as  prolist_revenue_d3ly,
        f.attributed_etsy_ads_revenue as  attributed_etsy_ads_revenue_d3ly,
        f.attributed_etsy_ads_revenue_not_charged as  attributed_etsy_ads_revenue_not_charged_d3ly,
        -- metrics with current multiplier
        f.gcp_costs_mult as gcp_costs_mult_d3ly,
        f.attributed_gms_mult as attributed_gms_mult_d3ly,
        f.attributed_attr_rev_mult as attributed_attr_rev_mult_d3ly,
        f.attributed_receipts_mult as attributed_receipts_mult_d3ly,
        f.attributed_new_receipts_mult as attributed_new_receipts_mult_d3ly,
        f.attributed_lapsed_receipts_mult as attributed_lapsed_receipts_mult_d3ly,
        f.attributed_existing_receipts_mult as attributed_existing_receipts_mult_d3ly,
        f.attributed_new_gms_mult as attributed_new_gms_mult_d3ly,
        f.attributed_lapsed_gms_mult as attributed_lapsed_gms_mult_d3ly,
        f.attributed_existing_gms_mult as attributed_existing_gms_mult_d3ly,
        f.attributed_new_attr_rev_mult as attributed_new_attr_rev_mult_d3ly,
        f.attributed_lapsed_attr_rev_mult as attributed_lapsed_attr_rev_mult_d3ly,
        f.attributed_existing_attr_rev_mult as attributed_existing_attr_rev_mult_d3ly,
        -- metrics with finance multiplier
        f.gcp_costs_mult_fin as gcp_costs_mult_fin_d3ly,
        f.attributed_gms_mult_fin as attributed_gms_mult_fin_d3ly,
        f.attributed_attr_rev_mult_fin as attributed_attr_rev_mult_fin_d3ly,
        f.attributed_receipts_mult_fin as attributed_receipts_mult_fin_d3ly,
        f.attributed_new_receipts_mult_fin as attributed_new_receipts_mult_fin_d3ly,
        f.attributed_lapsed_receipts_mult_fin as attributed_lapsed_receipts_mult_fin_d3ly,
        f.attributed_existing_receipts_mult_fin as attributed_existing_receipts_mult_fin_d3ly,
        f.attributed_new_gms_mult_fin as attributed_new_gms_mult_fin_d3ly,
        f.attributed_lapsed_gms_mult_fin as attributed_lapsed_gms_mult_fin_d3ly,
        f.attributed_existing_gms_mult_fin as attributed_existing_gms_mult_fin_d3ly,
        f.attributed_new_attr_rev_mult_fin as attributed_new_attr_rev_mult_fin_d3ly,
        f.attributed_lapsed_attr_rev_mult_fin as attributed_lapsed_attr_rev_mult_fin_d3ly,
        f.attributed_existing_attr_rev_mult_fin as attributed_existing_attr_rev_mult_fin_d3ly
from keys k
left join `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
on k.`date` = a.`date`
    and k.device = a.device
    and k.marketing_region = a.marketing_region
    and k.key_market = a.key_market
    and k.channel_group = a.channel_group
    and k.top_channel = a.top_channel
    and k.second_channel = a.second_channel
    and k.third_channel = a.third_channel
    and k.utm_medium = a.utm_medium
    and k.utm_source = a.utm_source
    and k.utm_campaign = a.utm_campaign
    and k.utm_custom2 = a.utm_custom2
    and k.utm_content = a.utm_content
    and k.landing_event = a.landing_event
    and k.visit_market = a.visit_market
left join
    (select date_add(`date`, interval 1 year) as date1year   -- this data needs to be grouped bec 1 year past 2/28 and 2/29 is same
        ,device
        ,marketing_region 
        ,key_market
        ,channel_group
        ,top_channel 
        ,second_channel 
        ,third_channel 
        ,utm_medium
        ,utm_source
        ,utm_campaign
        ,utm_custom2
        ,utm_content
        ,landing_event
        ,visit_market
        ,incrementality_multiplier_current
        ,incrementality_multiplier_finance
        ,sum(visits) as visits
        ,sum(insession_registrations) as insession_registrations
        ,sum(insession_converting_visits) as insession_converting_visits
        ,sum(insession_orders) as insession_orders
        ,sum(insession_gms) as insession_gms
        ,sum(new_visits) as new_visits
        ,sum(insession_new_buyers) as insession_new_buyers
        ,sum(insession_new_buyer_gms) as insession_new_buyer_gms
        ,sum(gcp_costs) as gcp_costs
        ,sum(gcp_costs_mult) as gcp_costs_mult
        ,sum(gcp_costs_mult_fin) as gcp_costs_mult_fin
        ,sum(attributed_gms) as attributed_gms
        ,sum(attributed_attr_rev) as attributed_attr_rev
        ,sum(attributed_receipts) as attributed_receipts
        ,sum(attributed_new_receipts) as attributed_new_receipts
        ,sum(attributed_lapsed_receipts) as attributed_lapsed_receipts
        ,sum(attributed_existing_receipts) as attributed_existing_receipts
        ,sum(attributed_new_gms) as attributed_new_gms
        ,sum(attributed_lapsed_gms) as attributed_lapsed_gms
        ,sum(attributed_existing_gms) as attributed_existing_gms
        ,sum(attributed_new_attr_rev) as attributed_new_attr_rev
        ,sum(attributed_lapsed_attr_rev) as attributed_lapsed_attr_rev
        ,sum(attributed_existing_attr_rev) as attributed_existing_attr_rev
        ,sum(prolist_revenue) as prolist_revenue
        ,sum(attributed_etsy_ads_revenue) as attributed_etsy_ads_revenue
        ,sum(attributed_etsy_ads_revenue_not_charged) as attributed_etsy_ads_revenue_not_charged
        ,sum(attributed_gms_mult) as attributed_gms_mult
        ,sum(attributed_attr_rev_mult) as attributed_attr_rev_mult
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
        ,sum(attributed_gms_mult_fin) as attributed_gms_mult_fin
        ,sum(attributed_attr_rev_mult_fin) as attributed_attr_rev_mult_fin
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
        from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
        where `date` < date_sub(date_sub(current_date, interval 1 year), interval 1 day)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17) b 
    on k.`date` = b.date1year
    and k.device = b.device
    and k.marketing_region = b.marketing_region
    and k.key_market = b.key_market
    and k.channel_group = b.channel_group
    and k.top_channel = b.top_channel
    and k.second_channel = b.second_channel
    and k.third_channel = b.third_channel
    and k.utm_medium = b.utm_medium
    and k.utm_source = b.utm_source
    and k.utm_campaign = b.utm_campaign
    and k.utm_custom2 = b.utm_custom2
    and k.utm_content = b.utm_content
    and k.landing_event = b.landing_event
    and k.visit_market = b.visit_market
left join `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` c
    on k.`date` = date_add(c.`date`, interval 7 day)
    and k.device = c.device
    and k.marketing_region = c.marketing_region
    and k.key_market = c.key_market
    and k.channel_group = c.channel_group
    and k.top_channel = c.top_channel
    and k.second_channel = c.second_channel
    and k.third_channel = c.third_channel
    and k.utm_medium = c.utm_medium
    and k.utm_source = c.utm_source
    and k.utm_campaign = c.utm_campaign
    and k.utm_custom2 = c.utm_custom2
    and k.utm_content = c.utm_content
    and k.landing_event = c.landing_event
    and k.visit_market = c.visit_market
    and c.`date` < date_sub(date_sub(current_date, interval 7 day), interval 1 day)
left join `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` d
    on k.`date` = date_add(d.`date`, interval 52 week)
    and k.device = d.device
    and k.marketing_region = d.marketing_region
    and k.key_market = d.key_market
    and k.channel_group = d.channel_group
    and k.top_channel = d.top_channel
    and k.second_channel = d.second_channel
    and k.third_channel = d.third_channel
    and k.utm_medium = d.utm_medium
    and k.utm_source = d.utm_source
    and k.utm_campaign = d.utm_campaign
    and k.utm_custom2 = d.utm_custom2
    and k.utm_content = d.utm_content
    and k.landing_event = d.landing_event
    and k.visit_market = d.visit_market
    and d.`date` < date_sub(date_sub(current_date, interval 52 week), interval 1 day)
left join `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` e
    on k.`date` = date_add(e.`date`, interval 104 week)
    and k.device = e.device
    and k.marketing_region = e.marketing_region
    and k.key_market = e.key_market
    and k.channel_group = e.channel_group
    and k.top_channel = e.top_channel
    and k.second_channel = e.second_channel
    and k.third_channel = e.third_channel
    and k.utm_medium = e.utm_medium
    and k.utm_source = e.utm_source
    and k.utm_campaign = e.utm_campaign
    and k.utm_custom2 = e.utm_custom2
    and k.utm_content = e.utm_content
    and k.landing_event = e.landing_event
    and k.visit_market = e.visit_market
    and e.`date` < date_sub(date_sub(current_date, interval 104 week), interval 1 day)
left join `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` f
    on k.`date` = date_add(f.`date`, interval 156 week)
    and k.device = f.device
    and k.marketing_region = f.marketing_region
    and k.key_market = f.key_market
    and k.channel_group = f.channel_group
    and k.top_channel = f.top_channel
    and k.second_channel = f.second_channel
    and k.third_channel = f.third_channel
    and k.utm_medium = f.utm_medium
    and k.utm_source = f.utm_source
    and k.utm_campaign = f.utm_campaign
    and k.utm_custom2 = f.utm_custom2
    and k.utm_content = f.utm_content
    and k.landing_event = f.landing_event
    and k.visit_market = f.visit_market
    and f.`date` < date_sub(date_sub(current_date, interval 156 week), interval 1 day)
);

create or replace view `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview`
as (
    select
        `date`,
        device,
        canonical_region,
        region,
        channel_group,
        top_level_channel,
        second_level_channel,
        third_level_channel,
        utm_medium,
        utm_source,
        utm_campaign,
        utm_custom2,
        utm_content,
        landing_event,
        visit_market,
        incrementality_multiplier_current,
        incrementality_multiplier_finance,
        visits,
        gcp_costs, 
        insession_registrations,
        insession_converting_visits,
        insession_orders,
        insession_gms,
        attributed_gms,
        attributed_attr_rev,
        attributed_receipts,
        attributed_new_receipts,
        attributed_lapsed_receipts,
        attributed_existing_receipts,
        attributed_new_gms,
        attributed_lapsed_gms,
        attributed_existing_gms,
        attributed_new_attr_rev,
        attributed_lapsed_attr_rev,
        attributed_existing_attr_rev,
        new_visits,
        insession_new_buyers,
        insession_new_buyer_gms,
        gcp_costs_mult,
        attributed_gms_mult,
        attributed_attr_rev_mult,
        attributed_receipts_mult,
        attributed_new_receipts_mult,
        attributed_lapsed_receipts_mult,
        attributed_existing_receipts_mult,
        attributed_new_gms_mult,
        attributed_lapsed_gms_mult,
        attributed_existing_gms_mult,
        attributed_new_attr_rev_mult,
        attributed_lapsed_attr_rev_mult,
        attributed_existing_attr_rev_mult,
        gcp_costs_mult_fin,
        attributed_gms_mult_fin,
        attributed_attr_rev_mult_fin,
        attributed_receipts_mult_fin,
        attributed_new_receipts_mult_fin,
        attributed_lapsed_receipts_mult_fin,
        attributed_existing_receipts_mult_fin,
        attributed_new_gms_mult_fin,
        attributed_lapsed_gms_mult_fin,
        attributed_existing_gms_mult_fin,
        attributed_new_attr_rev_mult_fin,
        attributed_lapsed_attr_rev_mult_fin,
        attributed_existing_attr_rev_mult_fin,
        -- ly
        incrementality_multiplier_current_ly,
        incrementality_multiplier_finance_ly,
        visits_ly,
        gcp_costs_ly,
        insession_registrations_ly,
        insession_converting_visits_ly,
        insession_orders_ly,
        insession_gms_ly,
        attributed_gms_ly,
        attributed_attr_rev_ly,
        attributed_receipts_ly,
        attributed_new_receipts_ly,
        attributed_lapsed_receipts_ly,
        attributed_existing_receipts_ly,
        attributed_new_gms_ly,
        attributed_lapsed_gms_ly,
        attributed_existing_gms_ly,
        attributed_new_attr_rev_ly,
        attributed_lapsed_attr_rev_ly,
        attributed_existing_attr_rev_ly,
        new_visits_ly,
        insession_new_buyers_ly,
        insession_new_buyer_gms_ly,
        gcp_costs_mult_ly,
        attributed_gms_mult_ly,
        attributed_attr_rev_mult_ly,
        attributed_receipts_mult_ly,
        attributed_new_receipts_mult_ly,
        attributed_lapsed_receipts_mult_ly,
        attributed_existing_receipts_mult_ly,
        attributed_new_gms_mult_ly,
        attributed_lapsed_gms_mult_ly,
        attributed_existing_gms_mult_ly,
        attributed_new_attr_rev_mult_ly,
        attributed_lapsed_attr_rev_mult_ly,
        attributed_existing_attr_rev_mult_ly,
        gcp_costs_mult_fin_ly,
        attributed_gms_mult_fin_ly,
        attributed_attr_rev_mult_fin_ly,
        attributed_receipts_mult_fin_ly,
        attributed_new_receipts_mult_fin_ly,
        attributed_lapsed_receipts_mult_fin_ly,
        attributed_existing_receipts_mult_fin_ly,
        attributed_new_gms_mult_fin_ly,
        attributed_lapsed_gms_mult_fin_ly,
        attributed_existing_gms_mult_fin_ly,
        attributed_new_attr_rev_mult_fin_ly,
        attributed_lapsed_attr_rev_mult_fin_ly,
        attributed_existing_attr_rev_mult_fin_ly,
        -- lw
        incrementality_multiplier_current_lw,
        incrementality_multiplier_finance_lw,
        visits_lw,
        gcp_costs_lw,
        insession_registrations_lw,
        insession_converting_visits_lw,
        insession_orders_lw,
        insession_gms_lw,
        attributed_gms_lw,
        attributed_attr_rev_lw,
        attributed_receipts_lw,
        attributed_new_receipts_lw,
        attributed_lapsed_receipts_lw,
        attributed_existing_receipts_lw,
        attributed_new_gms_lw,
        attributed_lapsed_gms_lw,
        attributed_existing_gms_lw,
        attributed_new_attr_rev_lw,
        attributed_lapsed_attr_rev_lw,
        attributed_existing_attr_rev_lw,
        new_visits_lw,
        insession_new_buyers_lw,
        insession_new_buyer_gms_lw,
        gcp_costs_mult_lw,
        attributed_gms_mult_lw,
        attributed_attr_rev_mult_lw,
        attributed_receipts_mult_lw,
        attributed_new_receipts_mult_lw,
        attributed_lapsed_receipts_mult_lw,
        attributed_existing_receipts_mult_lw,
        attributed_new_gms_mult_lw,
        attributed_lapsed_gms_mult_lw,
        attributed_existing_gms_mult_lw,
        attributed_new_attr_rev_mult_lw,
        attributed_lapsed_attr_rev_mult_lw,
        attributed_existing_attr_rev_mult_lw,
        gcp_costs_mult_fin_lw,
        attributed_gms_mult_fin_lw,
        attributed_attr_rev_mult_fin_lw,
        attributed_receipts_mult_fin_lw,
        attributed_new_receipts_mult_fin_lw,
        attributed_lapsed_receipts_mult_fin_lw,
        attributed_existing_receipts_mult_fin_lw,
        attributed_new_gms_mult_fin_lw,
        attributed_lapsed_gms_mult_fin_lw,
        attributed_existing_gms_mult_fin_lw,
        attributed_new_attr_rev_mult_fin_lw,
        attributed_lapsed_attr_rev_mult_fin_lw,
        attributed_existing_attr_rev_mult_fin_lw,
        -- dly
        incrementality_multiplier_current_dly,
        incrementality_multiplier_finance_dly,
        visits_dly,
        gcp_costs_dly,
        insession_registrations_dly,
        insession_converting_visits_dly,
        insession_orders_dly,
        insession_gms_dly,
        attributed_gms_dly,
        attributed_attr_rev_dly,
        attributed_receipts_dly,
        attributed_new_receipts_dly,
        attributed_lapsed_receipts_dly,
        attributed_existing_receipts_dly,
        attributed_new_gms_dly,
        attributed_lapsed_gms_dly,
        attributed_existing_gms_dly,
        attributed_new_attr_rev_dly,
        attributed_lapsed_attr_rev_dly,
        attributed_existing_attr_rev_dly,
        new_visits_dly,
        insession_new_buyers_dly,
        insession_new_buyer_gms_dly,
        gcp_costs_mult_dly,
        attributed_gms_mult_dly,
        attributed_attr_rev_mult_dly,
        attributed_receipts_mult_dly,
        attributed_new_receipts_mult_dly,
        attributed_lapsed_receipts_mult_dly,
        attributed_existing_receipts_mult_dly,
        attributed_new_gms_mult_dly,
        attributed_lapsed_gms_mult_dly,
        attributed_existing_gms_mult_dly,
        attributed_new_attr_rev_mult_dly,
        attributed_lapsed_attr_rev_mult_dly,
        attributed_existing_attr_rev_mult_dly,
        gcp_costs_mult_fin_dly,
        attributed_gms_mult_fin_dly,
        attributed_attr_rev_mult_fin_dly,
        attributed_receipts_mult_fin_dly,
        attributed_new_receipts_mult_fin_dly,
        attributed_lapsed_receipts_mult_fin_dly,
        attributed_existing_receipts_mult_fin_dly,
        attributed_new_gms_mult_fin_dly,
        attributed_lapsed_gms_mult_fin_dly,
        attributed_existing_gms_mult_fin_dly,
        attributed_new_attr_rev_mult_fin_dly,
        attributed_lapsed_attr_rev_mult_fin_dly,
        attributed_existing_attr_rev_mult_fin_dly,
        -- dlly
        incrementality_multiplier_current_dlly,
        incrementality_multiplier_finance_dlly,
        visits_dlly,
        gcp_costs_dlly,
        insession_registrations_dlly,
        insession_converting_visits_dlly,
        insession_orders_dlly,
        insession_gms_dlly,
        attributed_gms_dlly,
        attributed_attr_rev_dlly,
        attributed_receipts_dlly,
        attributed_new_receipts_dlly,
        attributed_lapsed_receipts_dlly,
        attributed_existing_receipts_dlly,
        attributed_new_gms_dlly,
        attributed_lapsed_gms_dlly,
        attributed_existing_gms_dlly,
        attributed_new_attr_rev_dlly,
        attributed_lapsed_attr_rev_dlly,
        attributed_existing_attr_rev_dlly,
        new_visits_dlly,
        insession_new_buyers_dlly,
        insession_new_buyer_gms_dlly,
        gcp_costs_mult_dlly,
        attributed_gms_mult_dlly,
        attributed_attr_rev_mult_dlly,
        attributed_receipts_mult_dlly,
        attributed_new_receipts_mult_dlly,
        attributed_lapsed_receipts_mult_dlly,
        attributed_existing_receipts_mult_dlly,
        attributed_new_gms_mult_dlly,
        attributed_lapsed_gms_mult_dlly,
        attributed_existing_gms_mult_dlly,
        attributed_new_attr_rev_mult_dlly,
        attributed_lapsed_attr_rev_mult_dlly,
        attributed_existing_attr_rev_mult_dlly,
        gcp_costs_mult_fin_dlly,
        attributed_gms_mult_fin_dlly,
        attributed_attr_rev_mult_fin_dlly,
        attributed_receipts_mult_fin_dlly,
        attributed_new_receipts_mult_fin_dlly,
        attributed_lapsed_receipts_mult_fin_dlly,
        attributed_existing_receipts_mult_fin_dlly,
        attributed_new_gms_mult_fin_dlly,
        attributed_lapsed_gms_mult_fin_dlly,
        attributed_existing_gms_mult_fin_dlly,
        attributed_new_attr_rev_mult_fin_dlly,
        attributed_lapsed_attr_rev_mult_fin_dlly,
        attributed_existing_attr_rev_mult_fin_dlly,
        -- d3ly
        incrementality_multiplier_current_d3ly,
        incrementality_multiplier_finance_d3ly,
        visits_d3ly,
        gcp_costs_d3ly,
        insession_registrations_d3ly,
        insession_converting_visits_d3ly,
        insession_orders_d3ly,
        insession_gms_d3ly,
        new_visits_d3ly,
        insession_new_buyers_d3ly,
        insession_new_buyer_gms_d3ly,
        attributed_gms_d3ly,
        attributed_attr_rev_d3ly,
        attributed_receipts_d3ly,
        attributed_new_receipts_d3ly,
        attributed_lapsed_receipts_d3ly,
        attributed_existing_receipts_d3ly,
        attributed_new_gms_d3ly,
        attributed_lapsed_gms_d3ly,
        attributed_existing_gms_d3ly,
        attributed_new_attr_rev_d3ly,
        attributed_lapsed_attr_rev_d3ly,
        attributed_existing_attr_rev_d3ly,
        prolist_revenue_d3ly,
        attributed_etsy_ads_revenue_d3ly,
        attributed_etsy_ads_revenue_not_charged_d3ly,
        gcp_costs_mult_d3ly,
        attributed_gms_mult_d3ly,
        attributed_attr_rev_mult_d3ly,
        attributed_receipts_mult_d3ly,
        attributed_new_receipts_mult_d3ly,
        attributed_lapsed_receipts_mult_d3ly,
        attributed_existing_receipts_mult_d3ly,
        attributed_new_gms_mult_d3ly,
        attributed_lapsed_gms_mult_d3ly,
        attributed_existing_gms_mult_d3ly,
        attributed_new_attr_rev_mult_d3ly,
        attributed_lapsed_attr_rev_mult_d3ly,
        attributed_existing_attr_rev_mult_d3ly,
        gcp_costs_mult_fin_d3ly,
        attributed_gms_mult_fin_d3ly,
        attributed_attr_rev_mult_fin_d3ly,
        attributed_receipts_mult_fin_d3ly,
        attributed_new_receipts_mult_fin_d3ly,
        attributed_lapsed_receipts_mult_fin_d3ly,
        attributed_existing_receipts_mult_fin_d3ly,
        attributed_new_gms_mult_fin_d3ly,
        attributed_lapsed_gms_mult_fin_d3ly,
        attributed_existing_gms_mult_fin_d3ly,
        attributed_new_attr_rev_mult_fin_d3ly,
        attributed_lapsed_attr_rev_mult_fin_d3ly,
        attributed_existing_attr_rev_mult_fin_d3ly,
        -- adjusted current
        attributed_gms_adjusted,
        attributed_attr_rev_adjusted,
        attributed_receipts_adjusted,
        attributed_new_receipts_adjusted,
        attributed_lapsed_receipts_adjusted,
        attributed_existing_receipts_adjusted,
        attributed_new_gms_adjusted,
        attributed_lapsed_gms_adjusted,
        attributed_existing_gms_adjusted,
        attributed_new_attr_rev_adjusted,
        attributed_lapsed_attr_rev_adjusted,
        attributed_existing_attr_rev_adjusted,
        attributed_gms_adjusted_lw,
        attributed_attr_rev_adjusted_lw,
        attributed_receipts_adjusted_lw,
        attributed_new_receipts_adjusted_lw,
        attributed_lapsed_receipts_adjusted_lw,
        attributed_existing_receipts_adjusted_lw,
        attributed_new_gms_adjusted_lw,
        attributed_lapsed_gms_adjusted_lw,
        attributed_existing_gms_adjusted_lw,
        attributed_new_attr_rev_adjusted_lw,
        attributed_lapsed_attr_rev_adjusted_lw,
        attributed_existing_attr_rev_adjusted_lw
        -- adjusted metrics with current multiplier
        ,attributed_gms_adjusted_mult,
        attributed_attr_rev_adjusted_mult,
        attributed_receipts_adjusted_mult,
        attributed_new_receipts_adjusted_mult,
        attributed_lapsed_receipts_adjusted_mult,
        attributed_existing_receipts_adjusted_mult,
        attributed_new_gms_adjusted_mult,
        attributed_lapsed_gms_adjusted_mult,
        attributed_existing_gms_adjusted_mult,
        attributed_new_attr_rev_adjusted_mult,
        attributed_lapsed_attr_rev_adjusted_mult,
        attributed_existing_attr_rev_adjusted_mult,
        attributed_gms_adjusted_mult_lw,
        attributed_attr_rev_adjusted_mult_lw,
        attributed_receipts_adjusted_mult_lw,
        attributed_new_receipts_adjusted_mult_lw,
        attributed_lapsed_receipts_adjusted_mult_lw,
        attributed_existing_receipts_adjusted_mult_lw,
        attributed_new_gms_adjusted_mult_lw,
        attributed_lapsed_gms_adjusted_mult_lw,
        attributed_existing_gms_adjusted_mult_lw,
        attributed_new_attr_rev_adjusted_mult_lw,
        attributed_lapsed_attr_rev_adjusted_mult_lw,
        attributed_existing_attr_rev_adjusted_mult_lw,
        -- adjusted metrics with finance multiplier
        attributed_gms_adjusted_mult_fin,
        attributed_attr_rev_adjusted_mult_fin,
        attributed_receipts_adjusted_mult_fin,
        attributed_new_receipts_adjusted_mult_fin,
        attributed_lapsed_receipts_adjusted_mult_fin,
        attributed_existing_receipts_adjusted_mult_fin,
        attributed_new_gms_adjusted_mult_fin,
        attributed_lapsed_gms_adjusted_mult_fin,
        attributed_existing_gms_adjusted_mult_fin,
        attributed_new_attr_rev_adjusted_mult_fin,
        attributed_lapsed_attr_rev_adjusted_mult_fin,
        attributed_existing_attr_rev_adjusted_mult_fin,
        attributed_gms_adjusted_mult_fin_lw,
        attributed_attr_rev_adjusted_mult_fin_lw,
        attributed_receipts_adjusted_mult_fin_lw,
        attributed_new_receipts_adjusted_mult_fin_lw,
        attributed_lapsed_receipts_adjusted_mult_fin_lw,
        attributed_existing_receipts_adjusted_mult_fin_lw,
        attributed_new_gms_adjusted_mult_fin_lw,
        attributed_lapsed_gms_adjusted_mult_fin_lw,
        attributed_existing_gms_adjusted_mult_fin_lw,
        attributed_new_attr_rev_adjusted_mult_fin_lw,
        attributed_lapsed_attr_rev_adjusted_mult_fin_lw,
        attributed_existing_attr_rev_adjusted_mult_fin_lw
    from
       `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted`
);

end;
