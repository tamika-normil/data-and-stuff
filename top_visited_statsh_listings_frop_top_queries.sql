with search_queries as 
    (SELECT
    query_sessions.query  AS query_sessions_query, 
    classified_taxonomy_id AS taxonomy_id,
    COUNT(*) AS query_sessions_query_sessions,
FROM `etsy-data-warehouse-prod`.search.query_sessions_new  AS query_sessions
INNER JOIN `etsy-data-warehouse-prod.weblog.visits`  AS weblog_visits ON query_sessions.visit_id = weblog_visits.visit_id AND query_sessions._date = weblog_visits._date
LEFT JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_profile`  AS mapped_user_profile ON weblog_visits.user_id = mapped_user_profile.mapped_user_id
LEFT JOIN `etsy-data-warehouse-prod.rollups.buyer_basics`
     AS buyer_basics ON buyer_basics.mapped_user_id = mapped_user_profile.mapped_user_id
WHERE LENGTH(query_sessions.query ) <> 0 AND (((( weblog_visits._date  ) >= ((DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -14 DAY))) AND ( weblog_visits._date  ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('UTC'), INTERVAL -14 DAY), INTERVAL 15 DAY))))) AND (buyer_basics.country ) = 'United States') AND ((1=1 -- no filter on 'query_sessions.week_ending'
  AND 1=1 AND weblog_visits._date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
      AND 1=1 -- no filter on 'query_sessions.week_ending'

      AND 1=1 -- no filter on 'query_sessions.date_date'

      AND 1=1 -- no filter on 'query_sessions.date_week'

      AND 1=1 -- no filter on 'query_sessions.date_month'

      AND 1=1 -- no filter on 'query_sessions.date_year'


    ) AND (query_sessions.query ) NOT LIKE '%gift%' AND ((query_sessions.query ) NOT LIKE '%personalize%' AND (query_sessions.query ) IS NOT NULL))
    group by 1,2),
search_queries_w_rank as 
  (select *, row_number() over (order by query_sessions_query_sessions desc) as query_popularity 
    from search_queries),

 active_stash_listings as (
      SELECT distinct l.listing_id
      FROM `etsy-data-warehouse-prod`.listing_mart.listings AS l
      LEFT OUTER JOIN `etsy-data-warehouse-prod`.etsy_shard.merch_listings AS m on l.listing_id = m.listing_id
      WHERE m.status = 0 AND is_active = 1
      ),
 transactions as 
    (select
        a.listing_id,
        l.title,
        tc.new_category as top_level_category,
        tc.second_level_cat_new as second_level_category,
        tc.third_level_cat_new as third_level_category,
        l.taxonomy_id,
        SUM(COALESCE( gms_net, 0)) AS listing_gms_net,
      from etsy-data-warehouse-prod.transaction_mart.all_transactions a
      join 
        etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans b on a.transaction_id = b.transaction_id
      left join
        active_stash_listings sl on a.listing_id = sl.listing_id
      left join 
        etsy-data-warehouse-prod.listing_mart.listing_vw l on a.listing_id = l.listing_id
      left join
        etsy-data-warehouse-prod.transaction_mart.all_transactions_categories tc on a.transaction_id = tc.transaction_id
      where a.listing_id is not null and a.listing_id > 0 
      and sl.listing_id is not null
      and b.date >= current_date - 30
      group by 1,2,3,4,5,6)
select s.*,t.*,  
image_urls.img_url  AS image_urls_image_url_small,
row_number() over (partition by query_sessions_query order by listing_gms_net desc) as listing_gms
from search_queries_w_rank s
left join transactions t using  (taxonomy_id)
LEFT JOIN `etsy-data-warehouse-prod.rollups.image_urls`  AS image_urls ON t.listing_id = image_urls.listing_id
where query_popularity <= 30 
qualify listing_gms <= 30
order by query_popularity asc, listing_gms asc 
