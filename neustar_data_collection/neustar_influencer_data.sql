-- Adele Kikuchi 
-- Neustar MMM Data Collection

-------------------------------------------
-- Influencer Collections Data Overview --
-------------------------------------------

create temporary table influencer_data as (
with base as (
  select 
  date(timestamp_seconds(run_date)) as date,
  key_market,
  slug, 
  program as type,
  sum(visits) as visits
  FROM `etsy-data-warehouse-prod.rollups.influencer_favorites_overview_toplevel` 
  where (slug_adjusted in ('dan-levy', 'gifting-roundup', 'gq-favorites', 'home-decor-roundup', 'iris-apfel', 'jessie-and-lennie', 'kelly-rowland', 'nicolerichie', 'roychoi-favorites', 'the-holderness-family-favorites', 'the-holderness-family-favorites-fathers-day', 'whoopi-goldberg-favorites')
  or lower(program) in ('etsy edit', 'collection')
  and lower(slug) <> 'holidaytastemakers-fr')
  group by 1,2,3,4
  order by visits desc
)
select 
date,
key_market as country,
sum( case when type = 'favorites' then visits else 0 end) as influencer_favorites_visits,
sum( case when type = 'etsy edit' then visits else 0 end) as influencer_etsy_edit_visits,
sum( case when type = 'collection' then visits else 0 end) as influencer_collection_visits,
sum(visits) as influencer_total_visits
from base 
where key_market in ('GB', 'CA', 'DE', 'FR', 'US') 
and date >= '2019-01-01'
group by 1,2
order by 1 desc
);

select * from influencer_data;


-- select min(date(timestamp_seconds(run_date))) as date from `etsy-data-warehouse-prod.rollups.influencer_favorites_overview` ;



--   select 
--   ep_page_title,
--   slug_adjusted, 
--   sum(visits) as visits
--   FROM `etsy-data-warehouse-prod.rollups.influencer_favorites_overview` 
-- --   where (slug_adjusted in ('sarahshermansamuel-etsyedit', 'prabal-gurung', 'iris-apfel', 'whoopi-goldberg-favorites', 'dan-levy', 'jessie-and-lennie', 'kelly-rowland', 'the-holderness-family-favorites') 
-- --   or (slug_adjusted like '%etsyedit')
-- --   or (lower(ep_page_title) like '%x etsy%')
-- --   or (lower(ep_page_title) like '%collab%')
-- --   or (lower(ep_page_title) like '%kollab%'))
--   group by 1,2 order by 3;


with base as (
select 
date_trunc(date,week(monday)) as week_beginning
,date_add(date_trunc(date,week(monday)) , interval 6 day) as week_ending
,country
,sum(influencer_favorites_visits) as influencer_favorites_visits
,sum(influencer_etsy_edit_visits) as influencer_etsy_edit_visits
,sum(influencer_collection_visits) as influencer_collection_visits
,sum(influencer_total_visits) as influencer_total_visits
from influencer_data
where date >= date_sub(current_date(), interval 3 quarter)
group by 1,2,3
) 
select 
extract(quarter from week_ending) as quarter
,min(week_beginning) as min_week_beginning
,max(week_ending) as max_week_ending
,country
,sum(influencer_favorites_visits) as influencer_favorites_visits
,sum(influencer_etsy_edit_visits) as influencer_etsy_edit_visits
,sum(influencer_collection_visits) as influencer_collection_visits
,sum(influencer_total_visits) as influencer_total_visits
from base 
where week_ending >= date_sub(current_date(), interval 2 quarter)
and extract(quarter from week_ending) != extract(quarter from current_date())
group by 1,4
order by 1,2,3,4 desc
;
