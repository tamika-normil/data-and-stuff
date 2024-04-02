begin 

CREATE OR REPLACE TEMP TABLE daily_gms AS (
  select date, mapped_user_id, sum(gms_net) as gms_net, count(distinct receipt_id) as receipts
  from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` 
  where market <> 'ipp'
  group by 1,2
);

-- stats of buyers who made a purchase since 2022
CREATE OR REPLACE TEMP TABLE buyer_segments AS (
  with set_date as (
    SELECT
      -- date('2020-12-31') as as_of_date, 
      -- end_date as as_of_date, 
      as_of_date
    FROM UNNEST( GENERATE_DATE_ARRAY('2020-01-01', DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH ) ) AS as_of_date
  ),
  purchase_stats as (
    SELECT
      a.mapped_user_id, 
      ex.as_of_date, 
      min(date) AS first_purchase_date, 
      max(date) AS last_purchase_date,
      coalesce(sum(gms_net),0) AS lifetime_gms,
      coalesce(count(DISTINCT date),0) AS lifetime_purchase_days, 
      coalesce(sum(receipts),0) AS lifetime_orders,
      round(cast(round(coalesce(sum(CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date - 1 THEN gms_net
      END), CAST(0 as NUMERIC)),20) as numeric),2) AS past_year_gms,
      count(DISTINCT CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date - 1 THEN date
      END) AS past_year_purchase_days,
      sum(CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date - 1 THEN receipts
      END) AS past_year_orders
    from `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
    cross join set_date ex
    join `etsy-data-warehouse-prod.user_mart.user_mapping` b
      on a.mapped_user_id = b.mapped_user_id
    join `etsy-data-warehouse-prod.user_mart.user_first_visits` c
      on b.user_id = c.user_id
    left join daily_gms e
      on a.mapped_user_id = e.mapped_user_id and e.date < ex.as_of_date
    GROUP BY 1,2
    having (ex.as_of_date >= min(date(timestamp_seconds(a.join_date))) or ex.as_of_date >= min(date(c.start_datetime)))
  )
  select
    mapped_user_id, 
    as_of_date,
    first_purchase_date,
    past_year_gms,
    past_year_purchase_days,
    lifetime_purchase_days,
    lifetime_gms,
    CASE  
      when p.lifetime_purchase_days = 0 or p.lifetime_purchase_days is null then 'Zero Time'  
      when date_diff(as_of_date,p.first_purchase_date, DAY)<=180 and (p.lifetime_purchase_days=2 or round(cast(round(p.lifetime_gms,20) as numeric),2) >100.00) then 'High Potential' 
      WHEN p.lifetime_purchase_days = 1 and date_diff(as_of_date,p.first_purchase_date, DAY) <=365 then 'OTB'
      when p.past_year_purchase_days >= 6 and p.past_year_gms >=200 then 'Habitual' 
      when p.past_year_purchase_days>=2 then 'Repeat' 
      when date_diff(as_of_date , p.last_purchase_date, DAY) >365 then 'Lapsed'
      else 'Active' 
      end as buyer_segment,
  from purchase_stats p
);

create or replace temp table exposed_to_ad as (
  with set_date as (
    SELECT
      -- date('2020-12-31') as as_of_date, 
      -- end_date as as_of_date, 
      as_of_date
    FROM UNNEST( GENERATE_DATE_ARRAY('2020-01-01', DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH ) ) AS as_of_date
  )
  select distinct
    a.mapped_user_id, 
    ex.as_of_date,   
    -- case when c.state = 1 then 'subscribed'
    --   when c.state = 0 and c.update_date-c.subscribe_date >= 1800 then 'unsubscribed'
    --   --allow 30 minutes between sub and unsub to register a true unsubscribe
    --   when c.state = 2 then 'limbo'
    --   else 'never_subscribed' end as sub_status,
    min(date(timestamp_seconds(a.join_date))) as join_date,
    min(date(c.start_datetime)) as first_visit_date,
    min(case when tactic = 'Influencer Subnetwork' then date(v.start_datetime) end) as exposed_subnetwork_date,
    min(case when tactic =  'Social Creator Co - CreatorIQ' then date(v.start_datetime) end) as exposed_ciq_date,
  from `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
  cross join set_date ex
  join `etsy-data-warehouse-prod.user_mart.user_mapping` b
      on a.mapped_user_id = b.mapped_user_id
  join `etsy-data-warehouse-prod.user_mart.user_first_visits` c
      on b.user_id = c.user_id
  left join etsy-data-warehouse-prod.buyatt_mart.visits v
      on b.user_id = v.user_id and v._date >= '2000-01-01' and date(v.start_datetime) <= last_day(ex.as_of_date, month)
  left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic p
      on v.utm_content = p.publisher_id and p.tactic in ('Social Creator Co - CreatorIQ','Influencer Subnetwork')
  group by 1,2
  having (ex.as_of_date >= min(date(timestamp_seconds(a.join_date))) or ex.as_of_date >= min(date(c.start_datetime)))
);

create or replace temp table first_visit as ( 
  select 
    a.mapped_user_id, 
    v.start_datetime, 
    row_number() over(partition by a.mapped_user_id order by v.start_datetime asc) as visit_order,
     case when mapped_platform_type like  '%mweb%android%' then "Android Mobile Web"
        when mapped_platform_type like '%boe%android%' then "Android BOE"
        when mapped_platform_type like '%mweb%ios%' then "iOS Mobile Web"
        when mapped_platform_type like '%boe%ios%' then "iOS BOE"
      else "Desktop" end as first_visit_device,
  from `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
  join `etsy-data-warehouse-prod.user_mart.user_mapping` b
      on a.mapped_user_id = b.mapped_user_id
  join `etsy-data-warehouse-prod.user_mart.user_first_visits` c
      on b.user_id = c.user_id
  join etsy-data-warehouse-prod.buyatt_mart.visits v
      on c.visit_id = v.visit_id and v._date >= '2000-01-01'
  qualify visit_order = 1
);

create or replace temp table first_purchase_day_transactions as ( 
  select 
    t.mapped_user_id, 
    t.date as first_purch_date, 
    t.transaction_id,
    a.creation_tsz, -- some users have multiple transactions on their first purchase day
    row_number() over(partition by t.mapped_user_id order by a.creation_tsz) as transaction_order,
    -- t.new_category,
    t.gms_net as gms,
  from
    `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
  join
    `etsy-data-warehouse-prod.transaction_mart.all_transactions` a USING(transaction_id)
  where 
    t.gms_net > 0
  qualify
    dense_rank() over(partition by t.mapped_user_id order by t.date) = 1 -- first purchase day only
);


CREATE OR REPLACE TEMP TABLE daily_visits AS (
  select date(v.start_datetime) as date, mapped_user_id, count(distinct visit_id) as visits, count(case when top_channel in ('us_paid','intl_paid') THEN visit_id end) as paid_visits 
  from `etsy-data-warehouse-prod.buyatt_mart.visits` v
  left join `etsy-data-warehouse-prod.user_mart.user_profile`  up on v.user_id = up.user_id
  where v.user_id is not null and v.user_id <> 0
    and date(v.start_datetime) >= DATE('2018-12-01')
    and _date >=  DATE('2018-12-01')
    and run_date >= UNIX_SECONDS(timestamp('2018-12-01'))
  group by 1,2
);

create or replace temp table historical_visits_data as ( 
  with set_date as (
    SELECT
      -- date('2020-12-31') as as_of_date, 
      -- end_date as as_of_date, 
      as_of_date,
    FROM UNNEST( GENERATE_DATE_ARRAY('2020-01-01', DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH ) ) AS as_of_date)
  select up.mapped_user_id, 
      ex.as_of_date,

      sum(CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date - 1 THEN visits END) AS past_year_visits,

      sum(CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date - 1 then paid_visits END) AS past_year_paid_visits,

      max(CASE
          WHEN date_trunc(date, month) = as_of_date THEN 1 else 0 END) AS visited_this_month,

      max(CASE
          WHEN v.date < ex.as_of_date THEN date END) date_last_visited

    from `etsy-data-warehouse-prod.user_mart.mapped_user_profile` up
    cross join set_date ex
    join daily_visits v on up.mapped_user_id = v.mapped_user_id and v.date <= last_day(ex.as_of_date, month)
    group by 1,2);


--some other stats like first visit channel and guest/registered 
create or replace temp table some_other_stats as ( 
select 
  mapped_user_id, 
  account_age, 
  case when guest_count > 0 and registered_user_count = 0 then 1 else 0 end as is_guest 
from 
  `etsy-data-warehouse-prod.user_mart.mapped_user_profile`
);

--select *
--from exposed_to_ad;

create or replace table `etsy-data-warehouse-dev.tnormil.exposed_subnetwork_history_new` as ( 
  select 
    b.mapped_user_id,
    b.as_of_date,
    b.buyer_segment,
    b.first_purchase_date,
    b.past_year_gms,
    b.past_year_purchase_days,
    b.lifetime_purchase_days,  
    b.lifetime_gms as ltv, 
    a.first_visit_date,
    
    a.exposed_subnetwork_date as exposed_subnetwork_date,
    case when a.exposed_subnetwork_date is not null then 1 else 0 end as exposed_subnetwork,
    
    a.exposed_ciq_date as exposed_ciq_date,
    case when a.exposed_ciq_date is not null then 1 else 0 end as exposed_ciq,

    case when date_trunc(a.exposed_subnetwork_date, month) = b.as_of_date then 1 else 0 end as exposed_subnetwork_firsttime,
    case when date_trunc(a.exposed_ciq_date, month) = b.as_of_date  then 1 else 0 end as exposed_ciq_firsttime,

    coalesce(bb.target_hhi, "Unknown") as target_hhi,
    coalesce(bb.target_gender, "Unknown") as target_gender,
    coalesce(bb.estimated_age, "Unknown") as estimated_age,
    coalesce(bb.first_order_category, "Unknown") as first_order_category,
    coalesce(bb.lifetime_top_category, "Unknown") as lifetime_top_category,
    case when coalesce(bb.country, "Unknown")  in ("United States","United Kingdom","France","Canada","Germany","Unknown") then country else 'ROW' end as country, 
    ss.account_age, 
    ss.is_guest,
    coalesce(v.first_visit_device,"Unknown") as first_visit_device,
    coalesce(hv.past_year_visits,0) as past_year_visits,
    coalesce(hv.past_year_paid_visits,0) as past_year_paid_visits,
    hv.date_last_visited,
    hv.visited_this_month, 
    sum(coalesce(f.gms,0)) as first_day_gms,
    max(case when a.exposed_subnetwork_date is not null or aa.exposed_subnetwork_date is not null then 1 else 0 end) as exposed_subnetwork_future,
    max(case when a.exposed_subnetwork_date is not null or aa.exposed_ciq_date is not null then 1 else 0 end) as exposed_ciq_future, 
  from 
    buyer_segments b 
  left join 
    exposed_to_ad a on b.mapped_user_id = a.mapped_user_id and b.as_of_date = a.as_of_date
   left join 
    exposed_to_ad aa on b.mapped_user_id = aa.mapped_user_id and date_add(b.as_of_date, interval 1 month) >= aa.as_of_date
  left join 
    `etsy-data-warehouse-prod.rollups.buyer_basics` bb on b.mapped_user_id = bb.mapped_user_id
  left join 
    first_purchase_day_transactions f on b.mapped_user_id = f.mapped_user_id
  left join 
    first_visit v on b.mapped_user_id = v.mapped_user_id
  left join 
    some_other_stats ss on b.mapped_user_id = ss.mapped_user_id
  left join 
    historical_visits_data hv on b.mapped_user_id = hv.mapped_user_id and b.as_of_date = hv.as_of_date
  where b.past_year_gms > 0 -- made a least 1 purchase in year
    -- and visited_this_month > 0
    -- and b.as_of_date = '2021-12-31' -- update year here.
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
);

end
