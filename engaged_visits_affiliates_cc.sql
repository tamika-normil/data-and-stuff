SELECT date(start_datetime) as date , 
 
case 
    when top_channel in ('direct', 'dark', 'internal', 'seo') then 'Non-Paid'
    when top_channel like 'social_%' then 'Non-Paid Social'
    when top_channel like 'email%' then 'CRM'
    when top_channel like 'push_%' then 'CRM'
    when top_channel in ('us_paid','intl_paid') then 
      case when (second_channel like '%gpla' or second_channel like '%bing_plas') then 'PLA/SEM'
        when (second_channel like '%google_ppc' or second_channel like '%intl_ppc%' or second_channel like '%bing_ppc' or second_channel like 'admarketplace') then 'PLA/SEM'
        when second_channel='affiliates'  and b.reporting_channel_group is not null then  b.reporting_channel_group 
        when second_channel='affiliates'  and b.reporting_channel_group is null then 'Affiliates'
        when (second_channel like 'facebook_disp%' or second_channel like 'pinterest_disp%') then 'Paid Social'
        when second_channel like '%native_display' or (utm_source = 'google' and utm_medium = 'cpc' and (utm_campaign like 'gdn%' or utm_campaign like 'gda%')) then 'Display'
        when second_channel in ('us_video','intl_video') then 'Midfunnel'
        else 'Other Paid'
      end
    else 'Non-Paid'
  end as reporting_channel_group,
  
utm_content,

case when utm_content =  '946733' then REGEXP_replace( utm_custom2 , r'(_p|_p_tiktok|_p_facebook|_tiktok|_meta)$','') else '0' end as subnetwork_id, 

count(visit_id) as visits, sum( engaged_visit_5mins ) as engaged_visit_5mins 
FROM `etsy-data-warehouse-prod.visit_mart.visits` t
left join `etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic` b on t.utm_content = b.publisher_id and t.second_channel = 'affiliates'
left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic apt on t.utm_content = apt.publisher_id
left join (select distinct affiliate_key from `etsy-data-warehouse-prod.etsy_shard.affiliate_users` where network_type = 3) uu on t.utm_custom2 = uu.affiliate_key
-- left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions cd
-- on t.top_channel = cd.top_channel
-- and t.second_channel = cd.second_channel
-- and t.third_channel = cd.third_channel
-- and utm_campaign_adjust = cd.utm_campaign
-- and t.utm_medium = cd.utm_medium
where _date >= "2023-01-01"
and run_date >= unix_seconds(timestamp('2023-01-01'))
and not ( platform in ("desktop","mobile_web") and bounced = 1 and is_screenless = 1 )
group by 1,2,3,4;
