#channel overview

with p_channel_overview as 
    (select date, sum(attributed_attr_rev) as attributed_attr_rev, sum(attributed_attr_rev_adjusted) as attributed_attr_rev_adjusted,
    sum(attributed_attr_rev_mult) as attributed_attr_rev_mult, sum(attributed_attr_rev_mult_fin) as attributed_attr_rev_mult_fin,
    sum(case when second_channel not in ('native_display') then visits * .0063 else 0 end) as gcp_costs_p,
    sum(case when second_channel not in ('native_display') then visits * incrementality_multiplier_current * .0063 else 0 end) as gcp_costs_mult_p,
    sum(case when second_channel not in ('native_display') then visits * incrementality_multiplier_finance * .0063 else 0 end) as gcp_costs_mult_fi_p
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview`
    group by 1),
d_channel_overview as 
    (select date, sum(attributed_attr_rev) as attributed_attr_rev_d, sum(attributed_attr_rev_adjusted) as attributed_attr_rev_adjusted_d,
    sum(attributed_attr_rev_mult) as attributed_attr_rev_mult_d, sum(attributed_attr_rev_mult_fin) as attributed_attr_rev_mult_fin_d, 
    sum(gcp_costs) as gcp_costs,
    sum(gcp_costs_mult) as gcp_costs_mult,
    sum(gcp_costs_mult_fin) as gcp_costs_mult_fi
    from `etsy-data-warehouse-dev.tnormil.channel_overview`
    group by 1)
select p.*, attributed_attr_rev_d, attributed_attr_rev_adjusted_d, attributed_attr_rev_mult_d, attributed_attr_rev_mult_fin_d, gcp_costs, gcp_costs_mult, gcp_costs_mult_fi
from p_channel_overview p
left join d_channel_overview d using (date)
order by 1 desc

#perf marketing s3

with gcp_costs_prod as
    (select date, sum(attributed_attr_rev) as attributed_attr_rev, sum(attributed_attr_rev_adjusted) as attributed_attr_rev_adjusted,
    sum(attributed_attr_rev_mult) as attributed_attr_rev_mult, sum(attributed_attr_rev_mult_fin) as attributed_attr_rev_mult_fin,
    sum(case when second_channel not in ('native_display') then visits * incrementality_multiplier_current * .0063 else 0 end) as gcp_costs_mult_p,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
    where (a.second_channel in ('facebook_disp','instagram_disp','facebook_disp_intl','gpla', 'google_ppc', 'bing_ppc', 'css_plas', 'intl_css_plas',
	  'bing_plas','intl_gpla','intl_ppc','intl_bing_ppc','intl_bing_plas','pinterest_disp','pinterest_disp_intl','us_video','intl_video', 'native_display', 'intl_native_display')
	   or (a.utm_source='admarketplace' and a.utm_medium='cpc'))
     and date>= DATE_SUB(current_date(), INTERVAL 36 DAY)
    group by 1),
  gcp_costs_dev as  
    (SELECT date_date as date, sum(gcp_costs_mult) as gcp_costs_mult
    FROM `etsy-data-warehouse-dev.buyatt_mart.perf_marketing_s3_data` 
    group by 1)
select *
from gcp_costs_prod
left join gcp_costs_dev using (date)
order by 1 desc
