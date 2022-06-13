with base as 
(SELECT listing_id, category,  illegal.value,
           split(illegal_term.key,'.')[SAFE_OFFSET(1)] as region,
           split(illegal_term.key,'.')[SAFE_OFFSET(2)] as language,
           array_to_string(value.value,'') as term
    FROM `etsy-data-warehouse-prod.olf.olf_hydration_daily` olf
             LEFT JOIN
         UNNEST(has_illegal_terms) illegal
             WITH
                 OFFSET
                     LEFT
             JOIN
         UNNEST(illegal_terms) illegal_term
             WITH
                 OFFSET
                     USING
    (
        OFFSET
    )
    LEFT JOIN UNNEST(illegal_term.value) value
     WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
      and listing_state = 'is_active'),
category_gms as
  (select *
  from 
  (select olf.category as category_opp_sizing, concat(replace(reporting_channel_group,' ',''), '_', replace(replace(engine,' ',''),'-','_')) as reporting_channel_group_engine, sum(coalesce(attr_gms,0)) as attr_gms
  from `etsy-data-warehouse-dev.rollups.perf_listings_sample_af` af
  left join (SELECT * FROM `etsy-data-warehouse-prod.olf.olf_hydration_daily` WHERE DATE(_PARTITIONTIME) = "2022-06-12") olf using (listing_id)
  where date_trunc(date,quarter) = '2022-01-01'
  group by 1,2) 
  PIVOT 
  ( -- #2 aggregate
   sum(attr_gms) AS q12022_attr_gms
  -- #3 pivot_column
  FOR reporting_channel_group_engine in ('PaidSocial_Pinterest_Paid', 'PaidSocial_Facebook_Paid', 'PLA_Connexity', 'PLA_Bing_Paid','Display_Bing_Paid', 'Affiliates_Affiliates', 'PLA_Google_Paid', 'Display_Google_Paid'))
    ),  
blocked_base as        
    (select category, count(distinct listing_id) as listings, count(distinct case when  value = true then listing_id end)
    blocked_listings
    , count(distinct case when  value = true and context in ('brand alignment','reputational') then listing_id end)
    brand_unsafe_listings
    from base a
    LEFT JOIN  `etsy-data-warehouse-prod.etsy_aux.compliance_blocklist_terms` b using (term, region, language)
    group by 1)
select b.*, c.*
from  blocked_base b
left join category_gms c on b.category = c.category_opp_sizing  ;
