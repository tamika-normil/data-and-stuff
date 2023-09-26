create temp table keys
partition by `date`
cluster by geo as (
 select distinct `date`, geo
from etsy-data-warehouse-dev.tnormil.google_metro_dma_perf
union distinct 
select distinct date_add(`date`, interval 1 year)  as `date`, geo
from etsy-data-warehouse-dev.tnormil.google_metro_dma_perf
where date_add(`date`, interval 1 year) < current_date);

with google_perf as 
(select
  k.`date`
  ,k.geo
  -- THIS YEAR
  ,a.response
  ,a.cost
  ,a.buyers        
  -- LAST YEAR
  ,b.response as response_ly
  ,b.cost as cost_ly
  ,b.buyers as buyer_ly 
  from keys k 
  left join `etsy-data-warehouse-dev.tnormil.google_metro_dma_perf` a
    on k.`date` = a.date
    and k.geo = a.geo
  left join 
   (select date_add(`date`, interval 1 year) as date1year   -- this data needs to be grouped bec 1 year past 2/28 and 2/29 is same
        ,geo
        ,sum(response) as response
        ,sum(cost) as cost
        ,sum(buyers) as buyers
        from `etsy-data-warehouse-dev.tnormil.google_metro_dma_perf`
        where `date` < date_sub(date_sub(current_date, interval 1 year), interval 1 day)
        group by 1,2) b  
    on k.`date` = b.date1year
    and k.geo = b.geo
  order by k.date desc),
google_spend_data as 
    (SELECT g.day as date, l.string_field_2 as geo, sum(case when Account_name
    = 'Etsy SEM US - Branded' then cast(Cost__Converted_currency_
    as float64) else 0 end) as sem_brand_cost
    , sum(case when Account_name
    like '% NB %' then cast(Cost__Converted_currency_
    as float64) else 0 end) as sem_nb_cost,
    sum(case when lower(Account_name)
    like '%shopping%' then cast(Cost__Converted_currency_
    as float64) else 0 end) as pla_cost,
    FROM etsy-data-warehouse-dev.tnormil.google_metro_dma_cost g
    left join etsy-data-warehouse-dev.tnormil.metro_lookup l on g.DMA_Region__Matched_ = l.string_field_0	
    group by 1,2),
agg_pre_period as 
    (SELECT coalesce(g.geo,cast(gd.geo as string)) as geo, 
    sum(g.buyers) as agg_buyers,
    --sum(g.clicks) as agg_clicks,
    --sum(g.impressions) as agg_impressions,
    sum(gd.buyers) as agg_gda_buyers, 
    sum(gd.clicks) as agg_gda_clicks, 
    sum(gd.impressions) as agg_gda_impressions,
    FROM etsy-data-warehouse-dev.tnormil.google_metro_dma_perf g
    left join etsy-data-warehouse-dev.tnormil.design_data_tbr d on g.geo = cast(d.geo as string) 
    left join etsy-data-warehouse-dev.tnormil.gda_metro_dma_perf gd on g.geo = cast(gd.geo as string) and g.date = gd.date
    where g.date >= '2023-04-01'
    and g.date < '2023-06-29'
    group by 1)
SELECT g.date, assignment, g.geo, case when assignment = 'Control' then 1 else 2 end as groupp, 

agg_buyers,
--agg_clicks,
--agg_impressions,
agg_gda_buyers, 
agg_gda_clicks, 
agg_gda_impressions,

sum(g.response) as response, sum(g.cost) as cost, 
sum(g.response_ly) as response_ly, sum(g.cost_ly) as cost_ly, 
sum(gd.response) as gda_response, sum(gd.cost) as gda_cost, 
sum(sem_brand_cost) as sem_b_cost,
sum(sem_nb_cost) as sem_nb_cost,
sum(pla_cost) as pla_cost
FROM google_perf g
left join etsy-data-warehouse-dev.tnormil.design_data_tbr d on g.geo = cast(d.geo as string) 
left join etsy-data-warehouse-dev.tnormil.gda_metro_dma_perf gd on g.geo = cast(gd.geo as string) and g.date = gd.date
left join google_spend_data gs on g.geo = gs.geo and g.date = gs.date
left join agg_pre_period a on g.geo = a.geo
--left join agg_excluded e on g.date = e.date
where assignment <> 'Excluded'
and g.date >= '2023-01-01'
and g.date <= '2023-08-01'
group by 1,2,3,4,5,6,7,8
order by 1 desc;

-- validate the data
with co as 
    (select date(purchase_date) as day, sum(attributed_attr_rev - coalesce(prolist_revenue,0)) as attributed_rev 
    from etsy-data-warehouse-prod.buyatt_rollups.channel_overview_restricted_purch_date
    where marketing_region = 'US'
    and purchase_date>='2023-04-01'
    group by 1),
daily_tracker as
    (select date(day) as day, sum(cost) as cost,  
    from etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker
    where engine = 'google'
    and lower(account_name) like '% us%' 
    and day>='2023-04-01'
    group by 1),
metro_dma as 
    (select date as day, sum(response) as dma_attributed_rev,sum(cost) as dma_cost,sum(buyers) as buyers
    from etsy-data-warehouse-dev.tnormil.google_metro_dma_perf
    group by 1)
select *
from daily_tracker
left join co using (day)
left join metro_dma using (day)
order by day desc; 
    

with daily_tracker as 
    (select date(day) as day, sum(attributed_rev_purch_date + coalesce(gcp_costs_mult,0) - coalesce(prolist_revenue,0)) as attributed_rev,sum(cost) as cost,  
    from etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker
    where engine = 'google'
    and reporting_channel_group = 'Display'
    and lower(account_name) like '% us%' 
    and day>='2023-01-01' and day <='2023-05-19'
    group by 1),
metro_dma as 
    (select date as day, sum(response) as dma_attributed_rev,sum(cost) as dma_cost,  
    from etsy-data-warehouse-dev.tnormil.gda_metro_dma_perf
    group by 1)
select *
from daily_tracker
left join metro_dma using (day);
