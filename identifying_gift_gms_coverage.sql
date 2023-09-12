with purchases as (
select
	a.date 
	, a.transaction_id 
	, t.trans_gms_net 
	, a.listing_id
	, a.is_gift 
	, tv.visit_id
from 
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions a 
left join 
	`etsy-data-warehouse-prod`.transaction_mart.transactions_gms_by_trans t 
using(transaction_id)
inner join 
	`etsy-data-warehouse-prod`.transaction_mart.transactions_visits tv 
on 
	a.transaction_id = tv.transaction_id
where 
	a.date between current_date - 365 and current_date ## past year
), gtt as (
select 
	a.* 
	, max(case when regexp_contains(l.title, "(?i)\bgift|\bcadeau|\bregalo|\bgeschenk|\bprezent|ギフト") then 1 else 0 end) as gift_title
	, max(case when regexp_contains(t.tag, "(?i)\bgift|\bcadeau|\bregalo|\bgeschenk|\bprezent|ギフト") then 1 else 0 end) as gift_tag
from 
	purchases a 
left join 
	`etsy-data-warehouse-prod`.listing_mart.listing_titles l 
using(listing_id) 
left join 
	`etsy-data-warehouse-prod`.etsy_shard.listings_tags t 
on 
	a.listing_id = t.listing_id
group by 1,2,3,4,5,6
), gift_searches as (
SELECT
	distinct visit_id
FROM `etsy-data-warehouse-prod.search.query_sessions_new` qs
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` qm USING (query)
WHERE _date >= current_date - 365
and is_gift > 0
)
select 
	sum(case when gift_title > 0 or gift_tag > 0 or b.visit_id is not null then trans_gms_net end)/sum(trans_gms_net) as gift_title_or_search
from 
	gtt a 
left join 
	gift_searches b 
on 
	a.visit_id = b.visit_id
;
