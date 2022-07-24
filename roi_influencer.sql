SELECT slug_adjusted, program, date_trunc(ep_publish_date, quarter) as date, sum(visits) as visits, sum(gms_direct) as gms_direct, sum(a.conversions_direct) as conversions_direct, sum(gms) as gms
FROM `etsy-data-warehouse-prod.rollups.influencer_favorites_overview_toplevel` a
where (program = 'collection' or  program = 'etsy edit' or slug_adjusted IN ('collection', 'dan-levy', 'etsy edit', 'gifting-roundup', 'gq-favorites', 'home-decor-roundup', 'iris-apfel', 'jessie-and-lennie', 'kelly-rowland', 'roychoi-favorites', 'the-etsy-house', 'the-holderness-family-favorites', 'the-holderness-family-favorites-fathers-day', 'whoopi-goldberg-favorites'))
and (slug <> 'holidaytastemakers-fr')
group by 1,2,3;

SELECT slug_adjusted, program, reporting_channel_group, sum(visits) as visits
FROM `etsy-data-warehouse-prod.rollups.influencer_favorites_overview_toplevel` a
left join `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` c using 
(utm_campaign,
utm_medium,
top_channel,
second_channel,
third_channel)
where program = 'collection' or  program = 'etsy edit' or slug_adjusted IN ('collection', 'dan-levy', 'etsy edit', 'gifting-roundup', 'gq-favorites', 'home-decor-roundup', 'iris-apfel', 'jessie-and-lennie', 'kelly-rowland', 'roychoi-favorites', 'the-etsy-house', 'the-holderness-family-favorites', 'the-holderness-family-favorites-fathers-day', 'whoopi-goldberg-favorites')
group by 1,2,3;

SELECT day, engine, sum(impressions) as impressions,sum(attributed_gms_mult_purch_date) as attributed_gms_mult_purch_date, 
sum(attr_gms) as gms, sum(cost) as spend, sum(attributed_receipts_mult_purch_date) as attributed_receipts_mult_purch_date
FROM `etsy-data-warehouse-prod.buyatt_rollups.performance_marketing_daily_tracker` 
where reporting_channel_group = 'Paid Social'
and engine in ('facebook','pinterest')
and date(day) > current_date - 60
group by 1,2
order by 2,1 desc;
