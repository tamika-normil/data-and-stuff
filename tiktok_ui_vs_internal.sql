with ui_clicks as 
(select date, sum(coalesce(Clicks__Destination_,0)) as clicks
from etsy-data-warehouse-dev.tnormil.tiktok_campaigns_ui
where Ad_Group_Name like '%Halloween%'
or Ad_Group_Name like '%TTCM Staycation 2023%'
group by 1),
internal as 
(select date, sum(coalesce(visits,0)) as visits 
from etsy-data-warehouse-prod.buyatt_rollups.channel_overview
where  utm_campaign in ('us_pros_cur_staycationttcm2023_staycationcons','us_pros_cur_staycationttcm2023_buyerandatclal')
or utm_medium = 'display_affiliates'
and lower(utm_source) = 'tiktok'
group by 1)
select coalesce(u.date, i.date) as date, sum(clicks) as clicks, sum(visits) as visits 
from ui_clicks u
full outer join internal i using (date)
group by 1;

with ui_clicks as 
(select date, sum(coalesce(Clicks__Destination_,0)) as clicks
from etsy-data-warehouse-dev.tnormil.tiktok_campaigns_ui
where Ad_Group_Name not like '%Halloween%'
and Ad_Group_Name not like '%TTCM Staycation 2023%'
group by 1),
internal as 
(select day as date, sum(coalesce(visits,0)) as visits 
from etsy-data-warehouse-prod.rollups.affiliates_tracker
where (subnetwork_id like '%_p%' or subnetwork_id like '%_p_tiktok%')
and subnetwork_id not like '%_p_meta%' 
group by 1)
select coalesce(u.date, i.date) as date, sum(clicks) as clicks, sum(visits) as visits 
from ui_clicks u
full outer join internal i using (date)
group by 1;
