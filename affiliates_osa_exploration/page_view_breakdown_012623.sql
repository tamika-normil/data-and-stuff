BEGIN
    DECLARE events_min_date DATE;
    DECLARE events ARRAY <STRING>;
    DECLARE event_map array <STRUCT<event STRING, map_value STRING>>;
    SET events_min_date = (
        SELECT min(_date) 
        FROM `etsy-data-warehouse-prod.weblog.events` e
    );

    set events = ['shop_home',
        'shop_items',
        'view_listing',
        'view_sold_listing',
        'view_unavailable_listing',
        'search',
        'async_listings_search',
        'browselistings',
        'home' , 
        'favorites_view' ,
        'cart_view',
        'yr_purchases' ,
        'finds_page' ,
        'your_favorite_shops',
        'market',
        'search_similar_items' ,
        'category_page' ,
        'discovery_feed',
        'moments_page' ,
        'convo_main' ,
        'convo_view' ,
        'homescreen',
        'recommended',
        'favorites_and_lists',
        'best_of_etsy',
        'native_category_page' ,
        'your_purchases' ,
        'your_favorite_items',
        'profile_favorite_listings_tab', 
        'profile_favorite_shops_tab',
        'view_favorite_shops',
        'async_favorite_shops',
        'help_with_order_view',
        'view_case'];

    set event_map = [struct('shop_items','shop_home'), 
          struct('recommended','for_you_etsy_picks'),
          struct('yr_purchases' , 'your_purchases'),
          struct('native_category_page' , 'category_page'),
          struct('favorites_and_lists' , 'your_favorite_items'),
          struct('async_favorite_shops' , 'view_favorite_shops'),
          struct('async_listings_search' , 'search'),
          struct('browselistings' , 'search')];

    WITH page_events AS (
      SELECT
        e._date,
        e.run_date,
        e.visit_id,
        coalesce((select map_value from unnest(event_map) where event = event_type), substr(event_type,1,50)) AS event_type,
        count(*) AS page_views
      FROM
        `etsy-data-warehouse-prod.weblog.events` AS e
         where e.event_type in unnest(events)
      GROUP BY 1, 2, 3, 4
    )
    select date_trunc(date, quarter) as quarter,
    case when tactic in ("Cashback", "Loyalty", "Loyalty Charity", "Coupon") then "Cashback/Loyalty/Coupon" 
    else tactic end as tactic,
    coalesce((select map_value from unnest(event_map) where event = landing_event_new), substr(landing_event_new,1,50)) AS event_type,
    sum(attributed_gms_adjusted) as attributed_gms_adjusted
    from (select *, case when landing_event in unnest(events) then landing_event else 'other' end as landing_event_new from `etsy-data-warehouse-prod.buyatt_rollups.channel_overview` 
    where second_channel = 'affiliates'
    and date >= '2019-01-01') v
    left join etsy-data-warehouse-prod.static.affiliates_publisher_by_tactic a on v.utm_content = a.publisher_id
    group by 1, 2, 3
    order by 1, 2, 3 ,4 desc;

end
