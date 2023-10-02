 create or replace table `etsy-data-warehouse-dev.tnormil.seller_acq` as
 ( with promo_code_user as 
  #if the same user fills out the form multiple times with different codes, use the entry from the first visit
      (select user_id,  REGEXP_EXTRACT(landing_event_url, r'promo_code=([^&]+)') AS promo_code, row_number() over (partition by user_id order by start_datetime asc) rnk from `etsy-data-warehouse-prod.weblog.visits` 
      where _date > '2023-09-01' and user_id is not null 
      qualify rnk = 1),
  sg_base as (
          select
          a.shop_id,
          a.shop_name,
          a.user_id,
          b.country,
          b.tactic,
          b.first_date,
          b.ads_credit_awarded_date,
          c.promo_code,
          COUNT(tactic) OVER (PARTITION BY b.user_id) as total_tactics_by_shop
        from
        `etsy-data-warehouse-prod.rollups.seller_basics` a
        inner join
        `etsy-data-warehouse-prod.rollups.optimised_sellers_upload` b on a.user_id = b.user_id
        left join promo_code_user c on a.user_id = c.user_id),
         sem_base as (
          select distinct
          a.shop_id,
          a.shop_name,
          a.user_id,
          c.promo_code,
          marketing_region as country,
          'SEM' as tactic,
          a.open_date,
          b.date as first_date,
          null as ads_credit_awarded_date,
          0 as total_tactics_by_shop
        from
        `etsy-data-warehouse-prod.rollups.seller_basics` a
        inner join
        `etsy-data-warehouse-prod.rollups.utm_shops` b on a.user_id = b.user_id
        inner join promo_code_user c on a.user_id = c.user_id),
        sem_driven_SG as (
        select
        a.shop_id,
        a.shop_name,
        a.user_id,
        a.country,
        a.tactic,
        open_date,
        coalesce(a.promo_code, b.promo_code) as promo_code,
        case when b.shop_id is not null then 'SEM Driven' else "Not SEM Driven" end as is_SEM_driven,
        coalesce((cast (a.first_date as date)), b.first_date) as first_date,
        a.ads_credit_awarded_date,
        a.total_tactics_by_shop
        from sg_base a left join sem_base b on a.shop_id = b.shop_id
        ),
        sem_only as
        (
        select
        a.*
        from sem_base a left join sem_driven_SG b on a.shop_id = b.shop_id
        where b.shop_id is null
        ),
        output as (
        select
        shop_id,
        shop_name,
        promo_code,
        user_id,
        country,
        tactic,
        open_date,
        is_SEM_driven,
        cast(first_date as timestamp) as first_date,
        cast(ads_credit_awarded_date as timestamp) as ads_credit_awarded_date,
        total_tactics_by_shop
        from sem_driven_SG
        union all
        select
        shop_id,
        shop_name,
        promo_code,
        user_id,
        country,
        tactic,
        open_date,
        'SEM Driven'  as is_SEM_driven,
        cast(first_date as timestamp) as first_date,
        null as ads_credit_awarded_date,
        total_tactics_by_shop
        from sem_only)
        select
        a.*,
        case when a.shop_id is not null and a.open_date is not null then 1 else 0 end as shops,
        b.high_potential_seller_status,
        b.primary_language,
        b.active_listings,
        b.about_page,
        b.seller_intent,
        b.has_ads_rev,
        b.top_category_new
        from output a
        inner join  `etsy-data-warehouse-prod.rollups.seller_basics` b
        on a.shop_id = b.shop_id
        where cast(first_date AS DATE) < current_date);

-- check that these are the counts you would expect
select first_date, sum(shops) as shops
from `etsy-data-warehouse-dev.tnormil.seller_acq`
group by 1
order by 1 desc

-- first_date	shops
-- 2023-09-29 00:00:00.000000 UTC	0
-- 2023-09-28 00:00:00.000000 UTC	9
-- 2023-09-27 00:00:00.000000 UTC	9
-- 2023-09-26 00:00:00.000000 UTC	15
-- 2023-09-25 00:00:00.000000 UTC	13
-- 2023-09-24 00:00:00.000000 UTC	17
-- 2023-09-23 00:00:00.000000 UTC	9
-- 2023-09-22 00:00:00.000000 UTC	20
-- 2023-09-21 00:00:00.000000 UTC	15
-- 2023-09-20 00:00:00.000000 UTC	19
