-- owner: tnormil@etsy.com
-- owner_team: performance-marketing@etsy.com
-- description: Data source for tiers associated with publishers based on historical perfromance data. 
-- access: etsy-data-warehouse-prod.rollups.affiliates_tiers= group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com
-- access: etsy-data-warehouse-prod.rollups.affiliate_history= group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com

begin 

create or replace temp table affiliate_history as
(with dates as 
    (SELECT distinct date
    FROM UNNEST(GENERATE_DATE_ARRAY(date_trunc('2021-01-01', day), current_date, INTERVAL 1 day)) AS date),
pubs_to_select as
    (select *
    from etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic
    where lower(tactic) = 'social creator co' or publisher_id = '946733'),
mmult as
    (SELECT month, utm_content as publisher_id, safe_divide(sum(attr_gms_total_new),sum(attr_gms_total_old)) mult
     FROM `etsy-data-warehouse-prod.rollups.awin_content_attribution_click` 
     group by 1,2),
aff_perf as 
    (select channel_overview_restricted.date as date, utm_content as publisher_id, utm_custom2 as subnetwork_id,
    COALESCE(SUM(( coalesce(channel_overview_restricted.visits, 0)  ) ), 0) as visits, 
    COALESCE(SUM(case when DATE(channel_overview_restricted.date)
    >= DATE('2022-04-11') then
    ( coalesce(channel_overview_restricted.attributed_gms_adjusted, 0)  )*0.115
    else ( coalesce(channel_overview_restricted.attributed_gms_adjusted, 0)  )*0.102
    end), 0) AS commission_revenue,
    COALESCE(SUM(( coalesce(channel_overview_restricted.prolist_revenue, 0)  ) ), 0) AS prolist_rev,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_etsy_ads_revenue, 0)  ) ), 0) AS attributed_etsy_ads_rev,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_attr_rev_adjusted, 0) )), 0) AS attributed_rev_estimated,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_gms_adjusted, 0) )), 0) AS attributed_gms_adjusted,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_new_gms_adjusted, 0) )), 0) AS attributed_new_gms_adjusted,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_lapsed_gms_adjusted, 0) )), 0) AS attributed_lapsed_gms_adjusted,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_receipts_adjusted, 0) )), 0) AS attributed_receipts_adjusted,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_new_receipts_adjusted, 0) )), 0) AS attributed_new_receipts_adjusted,
    COALESCE(SUM(( coalesce(channel_overview_restricted.attributed_lapsed_receipts_adjusted, 0) )), 0) AS attributed_lapsed_receipts_adjusted,
    FROM `etsy-data-warehouse-prod.buyatt_rollups.derived_channel_overview_restricted`  AS channel_overview_restricted 
    join pubs_to_select p on channel_overview_restricted.utm_content = p.publisher_id
    where second_level_channel = 'affiliates'
    group by 1,2,3),
awin_orders as 
    (select date(att.transaction_date) as date, cast(aw.publisher_id as string) as publisher_id, cast(click_ref as string) as subnetwork_id,count(distinct aw.order_ref) as awin_orders,
    count(distinct case when transaction_parts_commission_group_code = 'NEW' then aw.order_ref end) as awin_orders_new
    from etsy-data-warehouse-prod.marketing.awin_spend_data aw
    left join etsy-data-warehouse-prod.marketing.awin_transaction_data att using (order_ref)
    join pubs_to_select p on cast(aw.publisher_id as string)  = p.publisher_id
    group by 1,2,3),
awin_spend as 
    (select date_trunc(date(day),week) as date, account_name as publisher_id, subnetwork_id, sum(sales) as awin_sales, sum(cost) as awin_costs
    from etsy-data-warehouse-prod.rollups.affiliates_tracker
    group by 1,2,3),
click_active as 
    (select d.date, a.publisher_id, split(a.subnetwork_id,'_')[0] as subnetwork_id, max(case when visits > 0 then a.date end) as last_click_date
    from dates d
    join aff_perf a on a.date < d.date
    group by 1,2,3
    having last_click_date > date_sub(d.date, interval 104 week)),
summary as 
    (select 
    c.date, 
    c.publisher_id, 
    case when c.subnetwork_id is null then '0' else c.subnetwork_id end as subnetwork_id,
    concat(c.publisher_id, '-',case when c.subnetwork_id is null then '0' else  c.subnetwork_id end) as creator_id,
    awin_orders,
    awin_orders_new,
    awin_sales, 
    awin_costs,
    visits, 
    commission_revenue,
    prolist_rev,
    attributed_etsy_ads_rev,
    attributed_rev_estimated,
    attributed_gms_adjusted,
    attributed_new_gms_adjusted,
    attributed_lapsed_gms_adjusted,
    attributed_receipts_adjusted,
    attributed_new_receipts_adjusted,
    attributed_lapsed_receipts_adjusted,
    attributed_receipts_adjusted * coalesce(mult, 1) as attributed_receipts_adjusted_mult, 
    attributed_gms_adjusted * coalesce(mult, 1) as attributed_gms_adjusted_mult, 
    attributed_rev_estimated * coalesce(mult, 1) as attributed_rev_estimated_mult
    from click_active c  
    left join aff_perf a using (date, publisher_id, subnetwork_id)
    left join awin_orders o using (date, publisher_id, subnetwork_id)
    left join awin_spend s using (date, publisher_id,  subnetwork_id)
    left join mmult m on date_trunc(c.date, month) = date(m.month) and c.publisher_id = m.publisher_id),
creator_iq_log as 
    (SELECT PublisherId, DateJoinedPortal, row_number() over (partition by PublisherId order by PartitionDate desc) as rnk
    FROM `etsy-data-warehouse-prod.marketing.creator_iq_creators` 
    WHERE PartitionDate >= "2023-01-01" 
    qualify rnk = 1)
select s.*, c.DateJoinedPortal
from summary s
left join creator_iq_log c on s.subnetwork_id = cast(c.PublisherId as string));

select date, 
case when date(DateJoinedPortal) < '2023-06-01' or date(DateJoinedPortal) is null then 0 else 1 end as joined_recently,
count(distinct creator_id) as creators,  count(distinct case when visits > 0 then creator_id end) as click_active_creators, 
count(distinct case when awin_sales > 0 then creator_id end) as sales_active_creators, 
sum(visits) as visits,
sum(awin_orders) as awin_orders,
sum(awin_sales) as awin_sales, 
sum(awin_costs) as awin_costs,
sum(attributed_rev_estimated) as attributed_rev_estimated,
sum(attributed_gms_adjusted) as attributed_gms_adjusted,
sum(attributed_new_gms_adjusted) as attributed_new_gms_adjusted,
sum(attributed_lapsed_gms_adjusted) as attributed_lapsed_gms_adjusted,
sum(attributed_receipts_adjusted) as attributed_receipts_adjusted,
sum(attributed_new_receipts_adjusted) as attributed_new_receipts_adjusted,
sum(attributed_lapsed_receipts_adjusted) as attributed_lapsed_receipts_adjusted,
from affiliate_history
group by 1,2;

end;
