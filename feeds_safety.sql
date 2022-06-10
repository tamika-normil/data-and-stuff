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
      and listing_state = 'is_active')
select category, count(distinct listing_id) as listings, count(distinct case when  value = true then listing_id end)
blocked_listings
, count(distinct case when  value = true and context in ('brand alignment','reputational') then listing_id end)
brand_unsafe_listings
from base a
LEFT JOIN  `etsy-data-warehouse-prod.etsy_aux.compliance_blocklist_terms` b using (term, region, language)
group by 1;
