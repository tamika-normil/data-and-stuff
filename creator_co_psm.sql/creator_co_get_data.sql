begin 

#first visit or join dated on occured on after as_of_date

-- stats of buyers who made a purchase since 2022
CREATE OR REPLACE TEMP TABLE buyer_segments AS (
  with set_date as (
    SELECT
      -- date('2020-12-31') as as_of_date, 
      -- end_date as as_of_date, 
      as_of_date
    FROM UNNEST(ARRAY<DATE>[
      DATE('2023-12-31'), 
      DATE('2022-12-31'), 
      DATE('2021-12-31'),
      DATE('2020-12-31'),
      DATE('2019-12-31')]) AS as_of_date
  ),
  purchase_stats as (
    SELECT
      a.mapped_user_id, 
      ex.as_of_date, 
      min(date) AS first_purchase_date, 
      max(date) AS last_purchase_date,
      coalesce(sum(gms_net),0) AS lifetime_gms,
      coalesce(count(DISTINCT date),0) AS lifetime_purchase_days, 
      coalesce(count(DISTINCT receipt_id),0) AS lifetime_orders,
      round(cast(round(coalesce(sum(CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date THEN gms_net
      END), CAST(0 as NUMERIC)),20) as numeric),2) AS past_year_gms,
      count(DISTINCT CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date THEN date
      END) AS past_year_purchase_days,
      count(DISTINCT CASE
          WHEN date between date_sub(as_of_date, interval 365 DAY) and as_of_date THEN receipt_id
      END) AS past_year_orders
    from `etsy-data-warehouse-prod.user_mart.mapped_user_profile` a
    cross join set_date ex
    join `etsy-data-warehouse-prod.user_mart.user_mapping` b
      on a.mapped_user_id = b.mapped_user_id
    join `etsy-data-warehouse-prod.user_mart.user_first_visits` c
      on b.user_id = c.user_id
    left join  `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` e
      on a.mapped_user_id = e.mapped_user_id and e.date <= ex.as_of_date and market <> 'ipp'
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
    FROM UNNEST(ARRAY<DATE>[
      DATE('2023-12-31'), 
      DATE('2022-12-31'), 
      DATE('2021-12-31'),
      DATE('2020-12-31'),
      DATE('2019-12-31')]) AS as_of_date
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
      on b.user_id = v.user_id and v._date >= '2000-01-01' and date(v.start_datetime) <= ex.as_of_date 
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

--some other stats like first visit channel and guest/registered 
create or replace temp table some_other_stats as ( 
select 
  mapped_user_id, 
  account_age, 
  case when guest_count > 0 and registered_user_count = 0 then 1 else 0 end as is_guest 
from 
  `etsy-data-warehouse-prod.user_mart.mapped_user_profile`
);

select *
from exposed_to_ad;

create or replace temp table final as ( 
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
    case when a.exposed_subnetwork_date is not null then 1 else 0 end as exposed_ciq,
    case when a.exposed_subnetwork_date is not null or aa.exposed_subnetwork_date is not null then 1 else 0 end as exposed_subnetwork_ty_ny,
    case when a.exposed_subnetwork_date is not null or aa.exposed_ciq is not null then 1 else 0 end as exposed_ciq_ty_ny,
    coalesce(bb.target_hhi, "Unknown") as target_hhi,
    coalesce(bb.target_gender, "Unknown") as target_gender,
    coalesce(bb.estimated_age, "Unknown") as estimated_age,
    coalesce(bb.first_order_category, "Unknown") as first_order_category,
    coalesce(bb.lifetime_top_category, "Unknown") as lifetime_top_category,
    case when coalesce(bb.country, "Unknown")  in ("United States","United Kingdom","France","Canada","Germany","Unknown") then country else 'ROW' end as country, 
    ss.account_age, 
    ss.is_guest,
    coalesce(sum(f.gms),0) as first_day_gms, 
    coalesce(sum(v.first_visit_device),"Unknown") as first_visit_device
  from 
    buyer_segments b 
  left join 
    exposed_to_ad a using(mapped_user_id,as_of_date)
   left join 
    exposed_to_ad aa on b.mapped_user_id = aa.mapped_user_id and date_add(b.as_of_date, interval 1 year) = aa.as_of_date
  left join 
    `etsy-data-warehouse-prod.rollups.buyer_basics` bb using(mapped_user_id)
  left join 
    first_purchase_day_transactions f using(mapped_user_id)
  left join 
    first_visit v using (mapped_user_id)
  left join 
    some_other_stats ss using(mapped_user_id)
  where
    b.past_year_gms > 0 -- made a least 1 purchase in year
    -- and b.as_of_date = '2021-12-31' -- update year here.
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
);

--now let's sample the table so we can run PSM in R
create or replace table `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` as ( 
    select mapped_user_id,
    as_of_date,
    buyer_segment,
    first_purchase_date,
    --past_year_gms,
    --past_year_purchase_days,
    --lifetime_purchase_days,  
    --lifetime_gms as ltv, 
    target_hhi,
    target_gender,
    estimated_age,
    first_order_category,
    -- lifetime_top_category,
    country, 
    --ss.account_age, 
    is_guest,
    first_day_gms, 
    first_visit_device,
    first_visit_date,
  from 
    final
  where
     exposed_subnetwork_ty_ny <> 1
);

--now let's sample the table so we can run PSM in R
create or replace table `etsy-data-warehouse-dev.tnormil.exposed_subnetwork` as ( 
    select mapped_user_id,
    as_of_date,
    buyer_segment,
    first_purchase_date,
    --past_year_gms,
    --past_year_purchase_days,
    --lifetime_purchase_days,  
    --lifetime_gms as ltv, 
    target_hhi,
    target_gender,
    estimated_age,
    first_order_category,
    -- lifetime_top_category,
    country, 
    --ss.account_age, 
    is_guest,
    first_day_gms, 
    first_visit_device,
    first_visit_date,
  from 
    final
  where
     exposed_ciq_ty_ny <> 1 and as_of_date = '2023-12-01'
);

#could this go haywire for users with a join_date towards the end of the year or something?
#i wonder if i should look at users, 1 - 3 years join date, focused on all users that joined after the year x

end
