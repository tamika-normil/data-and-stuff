  (select campaign, sum(shop_visits * coalesce(cast(Shop_Lift_Estimate as float64),1)) as visits, sum(attr_receipt * coalesce(cast(Shop_Lift_Estimate as float64),1)) as attr_receipt
        from etsy-data-warehouse-prod.rollups.influencer_creator_drops_shops a
        join `etsy-data-warehouse-dev.tnormil.neustar_etl_influencer_mmm` b on lower(trim(a.influencer)) = lower(b.join_key)
        left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions using (utm_campaign,
utm_medium,			
top_channel,
second_channel,			
third_channel)
        where lower(touchpoint) like '%drop%'
        and lower(reporting_channel_group) not in ("email","push",'paid social', 'internal')
        and date between start_date and end_date
        group by 1);
