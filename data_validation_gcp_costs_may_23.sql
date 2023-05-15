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
