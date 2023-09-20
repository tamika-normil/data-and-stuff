with google_spend_data as 
    (SELECT g.day as date, l.string_field_2 as geo, sum(case when Account_name
    = 'Etsy SEM US - Branded' then cast(Cost__Converted_currency_
    as float64) else 0 end) as sem_brand_cost
    FROM etsy-data-warehouse-dev.tnormil.google_metro_dma_cost g
    left join etsy-data-warehouse-dev.tnormil.metro_lookup l on g.DMA_Region__Matched_ = l.string_field_0	
    group by 1,2),
agg_pre_period as 
    (SELECT coalesce(g.geo,cast(gd.geo as string)) as geo, 
    sum(g.buyers) as agg_buyers,
    sum(g.clicks) as agg_clicks,
    sum(g.impressions) as agg_impressions,
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
agg_clicks,
agg_impressions,
agg_gda_buyers, 
agg_gda_clicks, 
agg_gda_impressions,

sum(g.response) as response, sum(g.cost) as cost, 
-- sum(g.buyers) as buyers, sum(g.clicks) as clicks, sum(g.impressions) as impressions,
sum(gd.response) as gda_response, sum(gd.cost) as gda_cost, 
-- sum(gd.buyers) as gda_buyers, sum(gd.clicks) as gda_clicks, sum(gd.impressions) as gda_impressions,
sum(sem_brand_cost) as sem_b_cost
FROM etsy-data-warehouse-dev.tnormil.google_metro_dma_perf g
left join etsy-data-warehouse-dev.tnormil.design_data_tbr d on g.geo = cast(d.geo as string) 
left join etsy-data-warehouse-dev.tnormil.gda_metro_dma_perf gd on g.geo = cast(gd.geo as string) and g.date = gd.date
left join google_spend_data gs on g.geo = gs.geo and g.date = gs.date
left join agg_pre_period a on g.geo = a.geo
where assignment <> 'Excluded'
and g.date >= '2023-01-01'
group by 1,2,3,4,5,6,7,8,9,10
order by 1 desc;
