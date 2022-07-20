BEGIN 

#replace channel overview with dev version
with channel_overview_dev as
    (select  date_trunc(date, month) as month, 
    top_channel,
    second_channel,
    third_channel,
    channel_group,
#utm_custom2 as campaign_id,
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(attributed_attr_rev_adjusted) as rev,
    sum(visits) as visits,
    #sum(attributed_gms_ly) as gms_ly,
    #sum(attributed_gms_dly) as gms_dly,
    #sum(attributed_gms_dlly) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN(    'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp', 'pinterest_disp_intl', 'native_display', 'intl_native_display')
    group by 1,2,3,4,5),
channel_overview_prod as
    (select  date_trunc(date, month) as month, 
    top_channel,
    second_channel,
    third_channel,
    channel_group,
#utm_custom2 as campaign_id,
    sum(coalesce(attributed_gms_adjusted,0)) as gms,
    sum(attributed_attr_rev_adjusted) as rev,
    sum(visits) as visits,
    #sum(attributed_gms_ly) as gms_ly,
    #sum(attributed_gms_dly) as gms_dly,
    #sum(attributed_gms_dlly) as gms_dlly,
    from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` a
    WHERE `etsy-data-warehouse-prod.rollups.lower_case_ascii_only`(a.second_channel) IN(    'gpla', 'google_ppc', 'intl_gpla', 'intl_ppc', 'intl_bing_ppc', 'bing_ppc', 'bing_plas'
        ,'intl_bing_plas', 'affiliates', 'facebook_disp', 'instagram_disp', 'facebook_disp_intl'
        ,'us_video','intl_video', 'pinterest_disp', 'pinterest_disp_intl', 'native_display', 'intl_native_display')
    group by 1,2,3,4,5)
select coalesce(a.month,b.month) as month, 
    coalesce(a.top_channel,b.top_channel) as top_channel,
    coalesce(a.second_channel,b.second_channel) as second_channel,
    coalesce(a.third_channel,b.third_channel) as third_channel,
    coalesce(a.channel_group,b.channel_group) as channel_group,
safe_divide((a.gms-b.gms),b.gms) as gms,
safe_divide((a.rev-b.rev),b.rev) as rev,
safe_divide((a.visits-b.visits),b.visits) as visits,
#safe_divide((a.gms_ly-c.gms_ly),c.gms_ly) as gms_ly,
#safe_divide((a.gms_dly-c.gms_dly),c.gms_dly) as gms_dly,
#safe_divide((a.gms_dlly-c.gms_dlly),c.gms_dlly) as gms_dlly ,
from channel_overview_dev a
full outer join channel_overview_prod b using (month, top_channel, second_channel, third_channel, channel_group)
#where safe_divide((a.gms-b.gms),b.gms) or safe_divide((a.rev-b.rev),b.rev) or safe_divide((a.visits-b.visits),b.visits) >= .001
order by 2,1 desc;

END;
