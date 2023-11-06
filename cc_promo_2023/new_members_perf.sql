-- creator_iq_log as 
with creator_iq_log as 
    (SELECT CreatorID as PublisherId, Date_Joined, row_number() over (partition by CreatorID order by Date_Joined asc) as rnk
    FROM `etsy-data-warehouse-dev.tnormil.creatoriq_member_info` 
    qualify rnk = 1)
select date_joined, count(distinct PublisherId) as creators
from creator_iq_log
group by 1
order by 1 desc;

/*
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

create or replace table `etsy-data-warehouse-dev.tnormil.cc_increase_base_data`  as
(select date,
date_trunc(date,month) as month, 
date_trunc(date,week) as week, 
Date_Joined,
creator_id,
publisher_id,
subnetwork_id,
case when u.affiliate_key is not null or u.affiliate_key is not null then 'seller' else 'influencer' end as type,
case when publisher_id in ('852954') or subnetwork_id like '%13246459%' then 1 else 0 end as excludee, 
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
group by 1,2,3,4,5,6,7,8,9);
*/

#how many gms creator earned on average each mnh since joined
create or replace temp table  cc_increase_base_data  as
(with creator_iq_log as 
    (SELECT CreatorID as PublisherId, Date_Joined, row_number() over (partition by CreatorID order by Date_Joined asc) as rnk
    FROM `etsy-data-warehouse-dev.tnormil.creatoriq_member_info` 
    qualify rnk = 1),
dates as 
    (SELECT distinct date
    FROM UNNEST(GENERATE_DATE_ARRAY(date_trunc('2021-01-01', day), current_date, INTERVAL 1 day)) AS date),
dates_active as 
    (select d.date, a.publisherid, date_joined
    from dates d
    join creator_iq_log a on d.date > a.date_joined and d.date < date_add(a.date_joined, interval 3 month)
    where date_joined >= '2023-02-01')
select distinct a.date_joined, 
ceil(date_diff(a.date, a.date_joined, day)/30) as months_since_joined, publisherid, 
sum(coalesce(attributed_gms_adjusted,0)) over (partition by PublisherId order by ceil(date_diff(a.date, a.date_joined, day)/30) asc) as attributed_gms_adjusted,
sum(coalesce(attributed_receipts_adjusted,0)) over (partition by PublisherId order by ceil(date_diff(a.date, a.date_joined, day)/30) asc) as attributed_receipts_adjusted,
sum(coalesce(awin_sales,0)) over (partition by PublisherId order by ceil(date_diff(a.date, a.date_joined, day)/30) asc) as awin_sales,
from dates_active a 
left join `etsy-data-warehouse-dev.tnormil.cc_increase_base_data` b on cast(a.PublisherId as string) = b.subnetwork_id and a.date = b.date
order by publisherid, months_since_joined);

select date_trunc(date_joined, week) as week,
avg(case when months_since_joined = 1 and attributed_gms_adjusted > 0 then attributed_gms_adjusted end) as attributed_gms_adjusted_1,
avg(case when months_since_joined = 2 and attributed_gms_adjusted > 0 then attributed_gms_adjusted end) as attributed_gms_adjusted_2,
avg(case when months_since_joined = 3 and attributed_gms_adjusted > 0 then attributed_gms_adjusted end) as attributed_gms_adjusted_3,

avg(case when months_since_joined = 1 and awin_sales > 0 then awin_sales end) as awin_sales_1,
avg(case when months_since_joined = 2 and awin_sales > 0 then awin_sales end) as awin_sales_2,
avg(case when months_since_joined = 3 and awin_sales > 0 then awin_sales end) as awin_sales_3,

count(distinct publisherid) as creators,

--count(distinct case when months_since_joined = 1 and visits > 0 then publisherid end) as click_active_creators_1,
--count(distinct case when months_since_joined = 2 and visits > 0 then publisherid end) as click_active_creators_2,

--count(distinct case when months_since_joined = 1 and awin_sales > 0  then publisherid end) as awin_sales_creators_1,
--count(distinct case when months_since_joined = 2 and awin_sales > 0  then publisherid end) as awin_sales_creators_2,

from cc_increase_base_data
where publisherid not in (15515319)
group by 1;

select date_trunc(date_joined, month) as month, months_since_joined,
avg(case when attributed_gms_adjusted > 0 then attributed_gms_adjusted end) as attributed_gms_adjusted,

avg(case when awin_sales > 0 then awin_sales end) as awin_sales,

count(distinct publisherid) as creators,

from cc_increase_base_data
where publisherid not in (15515319)
group by 1,2;

(with creator_iq_log as 
    (SELECT CreatorID as PublisherId, Date_Joined, row_number() over (partition by CreatorID order by Date_Joined asc) as rnk
    FROM `etsy-data-warehouse-dev.tnormil.creatoriq_member_info` 
    qualify rnk = 1),
dates as 
    (SELECT distinct date
    FROM UNNEST(GENERATE_DATE_ARRAY(date_trunc('2021-01-01', day), current_date, INTERVAL 1 day)) AS date),
dates_active as 
    (select d.date, a.publisherid, date_joined
    from dates d
    join creator_iq_log a on d.date > a.date_joined
    where date_joined >= '2023-02-01')
select date_trunc(a.date, week) as week,
case when date_diff(a.date, a.date_joined, day) < 60 then 1 else 0 end as joined_recently,
count(distinct publisherid) as creators,
count(distinct case when visits > 0 then publisherid end) as click_active_creators, 
count(distinct case when attributed_gms_adjusted > 0 then publisherid end) as att_gms_active_creators, 
count(distinct case when awin_sales > 0 then publisherid end) as awin_sales_active_creators, 
from dates_active a 
left join `etsy-data-warehouse-dev.tnormil.cc_increase_base_data` b on cast(a.PublisherId as string) = b.subnetwork_id and a.date = b.date
group by 1,2
order by 1);

select date,week,case when date_diff(date, date_joined, day) < 60 then 1 else 0 end as joined_recently,
count(distinct creator_id) as creators, sum(sales_active_creators) as sales_active_creators, sum(click_active_creators) as  click_active_creators,
count(distinct case when attributed_gms_adjusted > 0 then creator_id end) as att_gms_active_creators, 
count(distinct case when awin_sales > 0 then creator_id end) as awin_sales_active_creators, 
sum(attributed_gms_adjusted) as attributed_gms_adjusted,
sum(attributed_receipts_adjusted) as attributed_receipts_adjusted
from `etsy-data-warehouse-dev.tnormil.cc_increase_base_data` 
group by 1,2,3

#aov
#visits
#1 month gms
#2 month gms
#3 month gms 
#1 month sales
#2 month sales
#3 month sales
#click active
#sales active
#att gms active
