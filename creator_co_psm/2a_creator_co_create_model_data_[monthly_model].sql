-- get model features from historical data

create or replace table `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_new` as (
    select a.mapped_user_id,
    a.as_of_date,
    a.buyer_segment,
    a.first_purchase_date,
    a.past_year_gms,
    a.past_year_purchase_days,
    a.lifetime_purchase_days,  
    a.ltv, 
    a.target_hhi,
    a.target_gender,
    a.estimated_age,
    a.first_order_category,
    a.lifetime_top_category,
    a.country, 
    --ss.account_age, 
    a.is_guest,
    a.first_day_gms, 
    a.first_visit_device,
    a.first_visit_date,
    
    case when a.exposed_subnetwork_firsttime = 1 then 1
    when a.exposed_subnetwork_firsttime = 0 and a.exposed_subnetwork_future = 0 and a.exposed_subnetwork = 0 then 0 end as exposed_subnetwork,
    a.exposed_subnetwork_date,
    
    date_diff(a.as_of_date, a.first_purchase_date, day) as days_since_first_purch,
    date_diff(a.as_of_date, a.first_visit_date, day) as days_since_first_visit,
    date_diff(a.as_of_date, a.date_last_visited, day) as days_since_last_visit,

    a.past_year_visits,
    a.past_year_paid_visits,
    round(safe_divide(a.past_year_paid_visits, a.past_year_visits) * 100,-1) as paid_share_visits

  from 
     `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_history_new`  a
  where a.as_of_date >= '2021-01-01'
);

-- sample records in the control for model

create or replace table `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_model_data`  as
(with counts as 
(select as_of_date, count(*) as cnt
from `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_new` 
where exposed_subnetwork = 0
group by 1),
base_w_rnk as
(SELECT e.*,
row_number() over (partition by as_of_date order by rand()) as rnk
FROM  `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_new` e
join counts c using (as_of_date)
where exposed_subnetwork = 0
and date_trunc(e.as_of_date,year) = '2022-01-01'
qualify rnk < cnt*.2)
select * except (rnk)
from base_w_rnk
union all
select *
from `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_new` 
where exposed_subnetwork = 1
and date_trunc(as_of_date,year) = '2022-01-01');
