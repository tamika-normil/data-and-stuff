-- owner: tnormil@etsy.com
-- owner_team: performance-marketing@etsy.com
-- description: Data source for tiers associated with publishers based on historical perfromance data. 
-- access: etsy-data-warehouse-prod.rollups.affiliates_tiers= group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com
-- access: etsy-data-warehouse-prod.rollups.affiliate_history= group:analysts-role@etsy.com, group:finance-accounting-role@etsy.com, group:ads-role@etsy.com, group:pattern-role@etsy.com, group:payments-role@etsy.com, group:shipping-role@etsy.com, group:kpi-role@etsy.com

begin 

#sellers/influencers 

create or replace temp table affiliate_history as
(with dates as 
    (SELECT distinct date
    FROM UNNEST(GENERATE_DATE_ARRAY(date_trunc('2021-01-01', day), current_date, INTERVAL 1 day)) AS date),
pubs_to_select as
    (select distinct publisher_id
    from etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic
    where lower(tactic) = 'social creator co' or publisher_id = '946733'),
mmult as
    (SELECT month, utm_content as publisher_id, safe_divide(sum(attr_gms_total_new),sum(attr_gms_total_old)) mult
     FROM `etsy-data-warehouse-prod.rollups.awin_content_attribution_click` 
     group by 1,2),
aff_perf as 
    (select channel_overview_restricted.date as date, utm_content as publisher_id, 
    case when publisher_id = '946733' then split(utm_custom2,'_')[0] else '0' end as subnetwork_id,
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
    and utm_custom2 not like '%_p%'
    group by 1,2,3),
more_attr as 
    (select date(start_datetime) as date, utm_content as publisher_id, 
    case when utm_content = '946733' then split(utm_custom2,'_')[0] else '0' end as subnetwork_id,
    sum(gms * paid_last_click_all) as gms_paid_last_click_all,
    sum(gms *	last_click_all) as gms_insess
    from etsy-data-warehouse-prod.buyatt_mart.visits v 
    join pubs_to_select p on v.utm_content = p.publisher_id
    left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on visit_id = ab.o_visit_id
    and timestamp_seconds(o_visit_run_date) >= '2021-01-01'
    where second_channel = 'affiliates'
    and _date >= '2021-01-01'
    group by 1,2,3),
awin_orders as 
    (select date(att.transaction_date) as date, cast(aw.publisher_id as string) as publisher_id, 
    case when cast(aw.publisher_id as string) = '946733' then split(cast(click_ref as string),'_')[0] else '0' end as subnetwork_id,count(distinct aw.order_ref) as awin_orders,
    count(distinct case when transaction_parts_commission_group_code = 'NEW' then aw.order_ref end) as awin_orders_new
    from etsy-data-warehouse-prod.marketing.awin_spend_data aw
    left join etsy-data-warehouse-prod.marketing.awin_transaction_data att using (order_ref)
    join pubs_to_select p on cast(aw.publisher_id as string)  = p.publisher_id
    where commission_status in ('pending','approved')
    and cast(click_ref as string) not like '%_p%'
    group by 1,2,3),
awin_spend as 
    (select date(day) as date, account_name as publisher_id,  split(subnetwork_id,'_')[0] as subnetwork_id, sum(sales) as awin_sales, sum(cost) as awin_costs
    from etsy-data-warehouse-prod.rollups.affiliates_tracker aw
    join pubs_to_select p on aw.account_name  = p.publisher_id
    where subnetwork_id  not like '%_p%'
    group by 1,2,3),
click_active as 
    (select d.date, a.publisher_id, subnetwork_id, max(case when visits > 0 then a.date end) as last_click_date
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
    attributed_rev_estimated * coalesce(mult, 1) as attributed_rev_estimated_mult,
    gms_paid_last_click_all,
    gms_insess
    from click_active c  
    left join aff_perf a using (date, publisher_id, subnetwork_id)
    left join awin_orders o using (date, publisher_id, subnetwork_id)
    left join awin_spend s using (date, publisher_id,  subnetwork_id)
    left join more_attr mo using (date, publisher_id,  subnetwork_id)
    left join mmult m on date_trunc(c.date, month) = date(m.month) and c.publisher_id = m.publisher_id),
creator_iq_log as 
    (SELECT CreatorID as PublisherId, Date_Joined, row_number() over (partition by CreatorID order by Date_Joined asc) as rnk
    FROM `etsy-data-warehouse-dev.tnormil.creatoriq_member_info` 
    qualify rnk = 1)
select s.*, c.Date_Joined
from summary s
left join creator_iq_log c on s.subnetwork_id = cast(c.PublisherId as string));

create temp table base_data as
(select date,
date_trunc(date,month) as month, 
case when date(Date_Joined) < '2023-05-18' or date(Date_Joined) is null then 0 else 1 end as joined_recently,
case when u.affiliate_key is not null or u.affiliate_key is not null then 'seller' else 'influencer' end as type,
case when publisher_id in ('852954') or subnetwork_id like '%13246459%' or creator_id in ('1171526-0') then 1 else 0 end as excludee, 
#'946733-13246457','946733-13246484'
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
sum(gms_paid_last_click_all) as gms_paid_last_click_all,
sum(gms_insess) as gms_insess
from affiliate_history ah
left join (select distinct affiliate_key from `etsy-data-warehouse-prod.etsy_shard.affiliate_users`) u on ah.publisher_id = u.affiliate_key
left join (select distinct affiliate_key from `etsy-data-warehouse-prod.etsy_shard.affiliate_users` where network_type = 3) uu on ah.subnetwork_id = uu.affiliate_key
group by 1,2,3,4,5);

create temp table keys
partition by `date` as (
 select distinct `date`, joined_recently, type, excludee
from base_data 
union distinct 
select distinct date_add(`date`, interval 1 year)  as `date`, joined_recently, type, excludee
from base_data 
where date_add(`date`, interval 1 year) < current_date);

(select
  k.`date`,
  date_trunc(k.date, month) as month,
  date_trunc(k.date, week) as week,
  k.joined_recently,
  k.type, 
  k.excludee,
  -- THIS YEAR
    a.creators,
    a.click_active_creators, 
    #a.click_active_creators_nojoindate, 
    a.sales_active_creators, 
    a.visits,
    a.awin_orders,
    a.awin_sales, 
    a.awin_costs,
    a.attributed_rev_estimated,
    a.attributed_gms_adjusted,
    a.attributed_new_gms_adjusted,
    a.attributed_lapsed_gms_adjusted,
    a.attributed_receipts_adjusted,
    a.attributed_new_receipts_adjusted,
    a.attributed_lapsed_receipts_adjusted, 
    a.gms_paid_last_click_all,
    a.gms_insess,
  -- LAST YEAR
    b.creators as creator_ly,
    b.click_active_creators as click_active_creators_ly, 
    #b.click_active_creators_nojoindate as click_active_creators_nojoindate_ly, 
    b.sales_active_creators as sales_active_creators_ly, 
    b.visits as visits_ly,
    b.awin_orders as awin_orders_ly,
    b.awin_sales as awin_sales_ly, 
    b.awin_costs as awin_costs_ly,
    b.attributed_rev_estimated as attributed_rev_estimated_ly,
    b.attributed_gms_adjusted as attributed_gms_adjusted_ly,
    b.attributed_new_gms_adjusted as attributed_new_gms_adjusted_ly,
    b.attributed_lapsed_gms_adjusted as attributed_lapsed_gms_adjusted_ly,
    b.attributed_receipts_adjusted as attributed_receipts_adjusted_ly,
    b.attributed_new_receipts_adjusted as attributed_new_receipts_adjusted_ly,
    b.attributed_lapsed_receipts_adjusted as attributed_lapsed_receipts_adjusted_ly,
    b.gms_paid_last_click_all as gms_paid_last_click_all_ly,
    b.gms_insess as gms_insess_ly,
  from keys k 
  left join base_data a
    on k.`date` = a.date
    and k.joined_recently = a.joined_recently
    and k.type = a.type
    and k.excludee = a.excludee
  left join 
   (select date_add(`date`, interval 1 year) as date1year,   -- this data needs to be grouped bec 1 year past 2/28 and 2/29 is same
        joined_recently,
         type, 
         excludee,
        sum(creators) as creators,  
        sum(click_active_creators) as click_active_creators, 
        #sum(click_active_creators_nojoindate) as click_active_creators_nojoindate, 
        sum(sales_active_creators) as sales_active_creators, 
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
        sum(gms_paid_last_click_all) as gms_paid_last_click_all,
        sum(gms_insess) as gms_insess
        from base_data
        where `date` < date_sub(date_sub(current_date, interval 1 year), interval 1 day)
        group by 1,2,3,4) b  
    on k.`date` = b.date1year
    and k.joined_recently = b.joined_recently
    and k.type = b.type
    and k.excludee = b.excludee
  order by k.date desc);

end;
