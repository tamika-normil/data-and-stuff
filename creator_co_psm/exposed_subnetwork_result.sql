begin

-- get pre and post period stats for matched and nonmatched users
-- yearly model

create temp table model_base as 
(with counts as 
(select as_of_date, count(*) as cnt
from `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` 
where exposed_subnetwork = 0
and preceding_as_of_date is not null
group by 1),
base_w_rnk as
(SELECT e.*,
row_number() over (partition by as_of_date order by rand()) as rnk
FROM  `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` e
join counts c using (as_of_date)
where exposed_subnetwork = 0
and preceding_as_of_date is not null
and e.as_of_date = '2021-12-31'
qualify rnk < cnt*.2)
select * except (rnk)
from base_w_rnk
union all
select *
from `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` 
where exposed_subnetwork = 1
and preceding_as_of_date is not null
and as_of_date = '2021-12-31');

create temp table model_base_win as 
with lim as 
    (select distinct PERCENTILE_CONT(past_year_gms, 0.95) OVER() as past_year_gms_lim, 
    PERCENTILE_CONT(visits, 0.95) OVER() as visits_lim, 
    PERCENTILE_CONT(past_year_purchase_days, 0.95) OVER() as past_year_purchase_days_lim, 
    PERCENTILE_CONT(ltv, 0.95) OVER() as ltv_lim,  
    PERCENTILE_CONT(lifetime_purchase_days, 0.95) OVER() as lifetime_purchase_days_lim
    from model_base)
select e.exposed_subnetwork,
e.mapped_user_id,
least(past_year_gms,past_year_gms_lim) as prior_past_year_gms_win,
least(visits,visits_lim) as prior_visits_win,
least(past_year_purchase_days,past_year_purchase_days_lim) as prior_past_year_purchase_days_win,
least(ltv,ltv_lim) as prior_ltv_win,
least(lifetime_purchase_days,lifetime_purchase_days_lim) as prior_lifetime_purchase_days_win,
from  `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` e
left join etsy-data-warehouse-dev.tnormil.matched_users_2023 m using (mapped_user_id)
left join lim on 1 = 1
where as_of_date = '2021-12-31';


create temp table future as 
(with lim_data  as 
( select 
      t.mapped_user_id, 
      sum(case when EXTRACT(YEAR from date) = EXTRACT(YEAR from preceding_as_of_date) then gms_net end) as prior_period_gms,
      sum(case when EXTRACT(YEAR from date) = EXTRACT(YEAR from as_of_date) then gms_net end) as next_yr_gms,

      count(distinct case when EXTRACT(YEAR from date) = EXTRACT(YEAR from preceding_as_of_date) then date end) as prior_purchase_days,
      count(distinct case when EXTRACT(YEAR from date) = EXTRACT(YEAR from as_of_date) then date end) as next_yr_purchase_days,

    from
      `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans`  t
      join model_base s on t.mapped_user_id = s.mapped_user_id
    where 
      gms_net > 0
      and as_of_date = '2021-12-31'
      and t.date >= date_trunc(preceding_as_of_date, year)
    group by 1 
  ) ,
get_lim as 
    (select distinct PERCENTILE_CONT(prior_period_gms, 0.95) OVER() as prior_period_gms_lim, 
    PERCENTILE_CONT(next_yr_gms, 0.95) OVER() as next_yr_gms_lim, 
    PERCENTILE_CONT(prior_purchase_days, 0.95) OVER() as prior_purchase_days_lim, 
    PERCENTILE_CONT(next_yr_purchase_days, 0.95) OVER() as next_yr_purchase_days_lim,  
    from lim_data),
base as (
    select 
      t.mapped_user_id, 
      sum(case when EXTRACT(YEAR from date) = EXTRACT(YEAR from preceding_as_of_date) then gms_net end) as prior_period_gms,
      sum(case when EXTRACT(YEAR from date) = EXTRACT(YEAR from as_of_date) then gms_net end) as next_yr_gms,

      count(distinct case when EXTRACT(YEAR from date) = EXTRACT(YEAR from preceding_as_of_date) then date end) as prior_purchase_days,
      count(distinct case when EXTRACT(YEAR from date) = EXTRACT(YEAR from as_of_date) then date end) as next_yr_purchase_days,

    from
      `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
      join `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` s on t.mapped_user_id = s.mapped_user_id
    where 
      gms_net > 0
      and as_of_date = '2021-12-31'
      and t.date >= date_trunc(preceding_as_of_date, year)
    group by 1 
  ) 
  select 
    m.mapped_user_id, 
    m.exposed_subnetwork, 
    m.buyer_segment,
    m.as_of_date,
    case when s.mapped_user_id is not null then 1 else 0 end as match, 

    least(coalesce(b.prior_period_gms,0),prior_period_gms_lim) as prior_period_gms_win,
    least(coalesce(b.next_yr_gms,0), next_yr_gms_lim) as next_yr_gms_win,

    least(coalesce(b.prior_purchase_days,0), prior_purchase_days_lim) as prior_purchase_days_win,
    least(coalesce(b.next_yr_purchase_days,0),next_yr_purchase_days_lim) as next_yr_purchase_days_win,

  from 
    `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` m -- csv output from R
  left join 
    `etsy-data-warehouse-dev.tnormil.matched_users_2023`  s using(mapped_user_id)
  left join 
    base b using(mapped_user_id)
  left join get_lim on 1 = 1
  where m.as_of_date = '2021-12-31');

create or replace table `etsy-data-warehouse-dev.tnormil.inc_exposed_subnetwork_result_tot` as
(select *
from future 
left join model_base_win using (mapped_user_id, exposed_subnetwork));

end 
