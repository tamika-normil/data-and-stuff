#compare channel overview to attr by browser for visits with a view_listing related landing event for Feeds validation 

CREATE OR REPLACE TEMPORARY table channel_overview AS
    (
    select date_trunc(date, month) as month, 
    case when top_channel in ('us_paid', 'intl_paid') then 'paid' else 'non paid' end as channel_group,
    sum(attributed_gms) as attr_gms,
    sum(attributed_attr_rev) as attr_rev, 
    sum(prolist_revenue) as prolist_revenue
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` channel_overview
    where ( date   >= DATE('2020-09-01')) AND ((UPPER(( channel_overview.landing_event  )) = UPPER('view_listing') OR UPPER(( channel_overview.landing_event  )) = UPPER('view_sold_listing') OR UPPER(( channel_overview.landing_event  )) = UPPER('view_unavailable_listing') OR UPPER(( channel_overview.landing_event  )) = UPPER('view_not_available_listing')))
    group by 1,2
    );

CREATE OR REPLACE TEMPORARY table listing_visits AS
    (
    select distinct visit_id
    from `etsy-data-warehouse-prod.analytics.listing_views` AS a
    where date_trunc(a._date, month) >= date_sub("2021-01-01", interval 1 month)
    and date_trunc(a._date, month) <= "2021-10-01"
    #and a.sequence_number <= 30
    ); 

CREATE OR REPLACE TEMPORARY table prolist_sum AS
    (
    select DATE_TRUNC(CAST(v.start_datetime AS DATE), MONTH) as month,
    case when top_channel in ('us_paid', 'intl_paid') then 'paid' else 'non paid' end as channel_group,
    sum(cost/100) as cost
    from `etsy-data-warehouse-prod.ads.prolist_click_visits` p 
    INNER JOIN `etsy-data-warehouse-prod.buyatt_mart.visits` v using (visit_id)
    #join listing_visits lv using (visit_id)
    where DATE_TRUNC(p._date, MONTH) >= DATE_SUB("2020-09-01", INTERVAL 1 MONTH)
    and landing_event in ('view_listing', 'view_unavailable_listing', 'view_sold_listing', 'view_not_available_listing')
    and DATE_TRUNC(CAST(v.start_datetime AS DATE), MONTH) >= "2020-09-01" 
    group by 1,2
    );   

with datruth as
    (
    select DATE_TRUNC(CAST(v.start_datetime AS DATE), MONTH) as month,
    case when top_channel in ('us_paid', 'intl_paid') then 'paid' else 'non paid' end as channel_group,
    count(distinct v.visit_id) as visits,
    sum(external_source_decay_all) AS attr_receipt,
    sum(external_source_decay_all * gms) AS attr_gms,
    sum((external_source_decay_all * attr_rev)) AS attr_rev,
    from `etsy-data-warehouse-prod.buyatt_mart.visits` AS v
    #join listing_visits lv using (visit_id)
    LEFT OUTER JOIN `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` AS ab ON v.visit_id = ab.o_visit_id 
    LEFT OUTER JOIN `etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv` clv on ab.receipt_id = clv.receipt_id
    where DATE_TRUNC(CAST(v.start_datetime AS DATE), MONTH) >= "2020-09-01" 
    and DATE_TRUNC(v._date, MONTH) >= "2020-09-01" 
    and landing_event in ('view_listing', 'view_unavailable_listing', 'view_sold_listing', 'view_not_available_listing') 
    group by 1,2
    )
select a.month,
a.channel_group,
a.attr_rev as raw_rev,
a.attr_gms as raw_gms,
 coalesce(ps.cost,0) as raw_prolist_revenue,
co.attr_rev as co_rev,
co.attr_gms as co_gms,
co.prolist_revenue as co_prolist_revenue
from datruth a
left join prolist_sum ps using (month,channel_group)
left join channel_overview co using (month,channel_group)
order by 2,1 ;
