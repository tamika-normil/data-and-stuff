create or replace table `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` as (
  with hist_dates as (
    select distinct mapped_user_id, as_of_date, LAG(as_of_date)
    OVER (PARTITION BY mapped_user_id ORDER BY as_of_date ASC) AS preceding_as_of_date
    from  `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_history`)
    select a.mapped_user_id,
    a.as_of_date,
    b.preceding_as_of_date,
    c.buyer_segment,
    c.first_purchase_date,
    c.past_year_gms,
    c.past_year_purchase_days,
    c.lifetime_purchase_days,  
    c.ltv, 
    c.target_hhi,
    c.target_gender,
    c.estimated_age,
    c.first_order_category,
    c.lifetime_top_category,
    c.country, 
    --ss.account_age, 
    c.is_guest,
    c.first_day_gms, 
    c.first_visit_device,
    c.first_visit_date,
    
    -- not exposed in previous year, but exposed to subnetworks in the next year
    case when a.exposed_subnetwork_firsttime = 1 then 1
    when a.exposed_subnetwork_firsttime = 0 and a.exposed_subnetwork_future = 0 and a.exposed_subnetwork = 0 then 0 end as exposed_subnetwork,
    c.exposed_subnetwork_date,
    
    date_diff(c.as_of_date, c.first_purchase_date, day) as days_since_first_purch,
    date_diff(c.as_of_date, c.first_visit_date, day) as days_since_first_visit,
    date_diff(c.as_of_date, c.date_last_visited, day) as days_since_last_visit,

    c.visits,
    c.paid_visits,
    round(safe_divide(c.paid_visits, c.visits) * 100,-1) as paid_share_visits

  from 
     `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_history`  a
  join hist_dates b using (mapped_user_id,as_of_date)
  left join  `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_history` c on a.mapped_user_id = c.mapped_user_id and b.preceding_as_of_date = c.as_of_date
  where a.as_of_date >= '2021-12-31'
);
