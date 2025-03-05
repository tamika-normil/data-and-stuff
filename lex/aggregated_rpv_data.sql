begin

-- NEW aggregated marketing revenue by experiement data

create temp table base_stats as 
    (select config_flag_param as experiment_id, variant_id, start_date, end_date, bucketing_id, count(distinct v.visit_id) as visits
    from `etsy-data-warehouse-dev.tnormil.lex_visit_level_exp` a
    left join etsy-data-warehouse-prod.weblog.visits v on a.bucketing_id = v.browser_id 
    and TIMESTAMP_TRUNC(a.bucketing_ts, SECOND) <= v.end_datetime
    and v._date BETWEEN start_date AND end_date
    and v._date >= '2024-01-01'
    group by 1,2,3,4,5);

create temp table prolist_stats as 
(with prolist as 
    (SELECT visit_id, sum(cost) / 100 as spend, 
    FROM `etsy-data-warehouse-prod.rollups.prolist_click_visits` pv
    WHERE _date >= '2024-01-01'
    group by 1),
decipher_prolist as 
    (SELECT pv.visit_id, max(coalesce(spend,0)) as prolist_revenue, max(case when ab.o_visit_id is not null then spend else 0 end) as converting_prolist_revenue, max(case when ab.o_visit_id is null then spend else 0 end) as non_converting_prolist_revenue
    FROM prolist pv
    left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on pv.visit_id = ab.o_visit_id 
    and o_visit_run_date >= unix_seconds('2024-01-01')
    group by 1)
select config_flag_param as experiment_id, variant_id, bucketing_id, sum(coalesce(prolist_revenue,0)) as prolist_revenue,
sum(coalesce(converting_prolist_revenue,0)) as converting_prolist_revenue, sum(coalesce(non_converting_prolist_revenue,0)) as non_converting_prolist_revenue
from `etsy-data-warehouse-dev.tnormil.lex_visit_level_exp` a
left join etsy-data-warehouse-prod.weblog.visits v on a.bucketing_id = v.browser_id 
and TIMESTAMP_TRUNC(a.bucketing_ts, SECOND) <= v.end_datetime
and v._date BETWEEN start_date AND end_date
and v._date >= '2024-01-01'
left join decipher_prolist p on v.visit_id = p.visit_id
group by 1,2,3);

create temp table tot_rev_stats as 
    (select config_flag_param as experiment_id, a.variant_id, bucketing_id, sum(coalesce(last_click_all*attr_rev,0)) as rev, sum(coalesce(last_click_all*etsy_ads_revenue,0)) as osa_rev, sum(coalesce(last_click_all*ltv_rev,0)) as ltv_rev,
    sum(coalesce(last_click_all*(attr_rev - ltv_rev - etsy_ads_revenue),0)) as comm_rev,  sum(coalesce(last_click_all*gms,0)) as gms, sum(last_click_all) as cvr
    from `etsy-data-warehouse-dev.tnormil.lex_visit_level_exp` a
    left join etsy-data-warehouse-prod.weblog.visits v on 
    a.bucketing_id = v.browser_id 
    and TIMESTAMP_TRUNC(a.bucketing_ts, SECOND) <= v.end_datetime
    and v._date BETWEEN start_date AND end_date
    and v._date >= '2024-01-01'
    left join etsy-data-warehouse-prod.buyatt_mart.attr_by_browser ab on v.visit_id = ab.o_visit_id 
    and TIMESTAMP_TRUNC(a.bucketing_ts, SECOND) <= ab.receipt_timestamp
    left join etsy-data-warehouse-prod.buyatt_mart.buyatt_analytics_clv clv on ab.receipt_id = clv.receipt_id
    group by 1,2,3);

create temp table experiement_sum as 
    (select a.*, 
    prolist_revenue,
    converting_prolist_revenue,
    non_converting_prolist_revenue,
    rev,
    ltv_rev,
    comm_rev,
    osa_rev,
    gms,
    cvr,
    coalesce(prolist_revenue,0) + coalesce(rev,0) as total_rev,
    coalesce(prolist_revenue,0) + coalesce(rev,0) - coalesce( non_converting_prolist_revenue,0) as total_rev_ex_ncp,
    PERCENTILE_CONT(gms, 0.995) OVER (PARTITION BY experiment_id, variant_id) AS gms_995,
    PERCENTILE_CONT( coalesce(prolist_revenue,0) + coalesce(rev,0) , 0.995) OVER (PARTITION BY experiment_id, variant_id) AS total_rev_995,
    PERCENTILE_CONT( coalesce(prolist_revenue,0) + coalesce(rev,0) - coalesce( non_converting_prolist_revenue,0) , 0.995) OVER (PARTITION BY experiment_id, variant_id) AS total_rev_ex_ncp_995,
    from base_stats a
    left join prolist_stats b using (experiment_id, variant_id, bucketing_id)
    left join tot_rev_stats c using (experiment_id, variant_id, bucketing_id)
    );

create or replace table `etsy-data-warehouse-dev.tnormil.lex_dev_catapult_stat_0224` as 
select experiment_id, variant_id, 
start_date,
end_date,
count(distinct bucketing_id) as bucketing_id, 
sum(visits) as visits,
safe_divide( COUNTIF(cvr > 0) , count(distinct bucketing_id) ) as cvr,
avg(prolist_revenue) as mean_prolist_rev,
avg(converting_prolist_revenue) as mean_cvr_prolist_rev,
avg(ltv_rev) as mean_ltv_rev,
avg(osa_rev) as mean_osa_rev,
avg(comm_rev) as mean_comm_rev,
avg(total_rev) as mean_total_rev,
avg(total_rev_ex_ncp) as mean_total_rev_ex_ncp,
safe_divide( sum( LEAST(total_rev, total_rev_995) ) , COUNTIF(total_rev > 0) ) as mean_total_rev_cb,
safe_divide( sum( LEAST(total_rev_ex_ncp, total_rev_ex_ncp_995) ) , COUNTIF(total_rev > 0) ) as mean_total_rev_ex_ncp_cb,
safe_divide( sum( LEAST(gms, gms_995) ) , COUNTIF(cvr > 0) ) as mean_aov_cb,
stddev_samp(prolist_revenue) as std_prolist_rev,
stddev_samp(converting_prolist_revenue) as std_cvr_prolist_rev,
stddev_samp(ltv_rev) as std_ltv_rev,
stddev_samp(osa_rev) as std_osa_rev,
stddev_samp(comm_rev) as std_comm_rev,
stddev_samp(total_rev) as std_total_rev,
stddev_samp(total_rev_ex_ncp) as std_total_rev_ex_ncp,
stddev_samp(case when total_rev > 0 then LEAST(total_rev, total_rev_995) end) as std_total_rev_cb,
stddev_samp(case when total_rev > 0 then LEAST(total_rev_ex_ncp, total_rev_ex_ncp_995) end) as std_total_rev_ex_ncp_cb,
stddev_samp(case when cvr > 0 then LEAST(gms, gms_995) end) as std_aov_cb,
sum( coalesce(prolist_revenue,0) )  as prolist_rev,
sum( coalesce(osa_rev,0) ) as osa_rev,
sum( coalesce(rev,0) ) as rev,
from experiement_sum
group by 1,2,3,4;

end
