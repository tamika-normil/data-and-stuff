SELECT date(date_trunc(receipt_timestamp, day)) as date, reporting_channel_group,
case when key_market in ('US') then 'US' else 'INTL' end as US_INTL,
  sum(gms_net) as gms_net,
  sum(r.gms_gross) as gms_gross
  FROM etsy-data-warehouse-prod.buyatt_mart.visits v
  left join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` ab on v.visit_id = ab.o_visit_id 
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using ( utm_campaign, utm_medium, top_channel, second_channel, third_channel)
  left join etsy-data-warehouse-prod.transaction_mart.receipts_gms r using (receipt_id)
  where ab.o_visit_run_date >= UNIX_SECONDS(CAST(CAST(DATE '2022-01-01' as DATETIME) AS TIMESTAMP))
    AND ab.o_visit_run_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
    and v._date >= DATE '2022-11-01'
    and v.top_channel in ('us_paid', 'intl_paid')
group by 1,2,3;

with detail as
(SELECT date(date_trunc(receipt_timestamp, day)) as date, reporting_channel_group,
coalesce(CAST(regexp_extract(landing_event_url, '(\?i)listing\\/(\\d{1,9})', 1, 1) as INT64), -1) AS listing_id,
case when key_market in ('US') then 'US' else 'INTL' end as US_INTL,
  sum(gms_net) as gms_net,
  sum(r.gms_gross) as gms_gross
  FROM etsy-data-warehouse-prod.buyatt_mart.visits v
  left join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` ab on v.visit_id = ab.o_visit_id 
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using ( utm_campaign, utm_medium, top_channel, second_channel, third_channel)
  left join etsy-data-warehouse-prod.transaction_mart.receipts_gms r using (receipt_id)
  where ab.buy_date >= UNIX_SECONDS(CAST(CAST(DATE '2023-01-01' as DATETIME) AS TIMESTAMP))
    AND ab.buy_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
    and _date >= DATE '2022-11-01'
    and v.top_channel in ('us_paid', 'intl_paid')
    and reporting_channel_group = 'Display'
    and landing_event in ('view_listing')
group by 1,2,3,4)
SELECT date, reporting_channel_group,
top_category,
US_INTL,
  sum(gms_net) as gms_net,
  sum(gms_gross) as gms_gross
  FROM detail
  left join etsy-data-warehouse-prod.listing_mart.listing_vw using (listing_id)
group by 1,2,3,4;

with detail as
(SELECT date(date_trunc(receipt_timestamp, day)) as date, reporting_channel_group,
coalesce(CAST(regexp_extract(landing_event_url, '(\?i)listing\\/(\\d{1,9})', 1, 1) as INT64), -1) AS listing_id,
case when key_market in ('US') then 'US' else 'INTL' end as US_INTL,
  sum(gms_net) as gms_net,
  sum(r.gms_gross) as gms_gross
  FROM etsy-data-warehouse-prod.buyatt_mart.visits v
  left join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` ab on v.visit_id = ab.o_visit_id 
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using ( utm_campaign, utm_medium, top_channel, second_channel, third_channel)
  left join etsy-data-warehouse-prod.transaction_mart.receipts_gms r using (receipt_id)
  where ab.buy_date >= UNIX_SECONDS(CAST(CAST(DATE '2023-01-01' as DATETIME) AS TIMESTAMP))
    AND ab.buy_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
    and _date >= DATE '2022-11-01'
    and v.top_channel in ('us_paid', 'intl_paid')
    and reporting_channel_group = 'Display'
    and landing_event in ('view_listing')
group by 1,2,3,4)
SELECT date, reporting_channel_group,
listing_id,
  sum(gms_net) as gms_net,
  sum(gms_gross) as gms_gross
  FROM detail
  left join etsy-data-warehouse-prod.listing_mart.listing_vw using (listing_id)
  where date = '2023-06-18'
group by 1,2,3;

/*
case clicked_listing_id,
  sum(gms_net) as gms_net,
  sum(r.gms_gross) as gms_gross
  FROM etsy-data-warehouse-prod.buyatt_mart.visits v
  left join `etsy-data-warehouse-prod.buyatt_mart.attr_by_browser` ab on v.visit_id = ab.o_visit_id 
  left join etsy-data-warehouse-prod.buyatt_mart.channel_dimensions c using ( utm_campaign, utm_medium, top_channel, second_channel, third_channel)
  left join etsy-data-warehouse-prod.transaction_mart.receipts_gms r using (receipt_id)
  where ab.buy_date >= UNIX_SECONDS(CAST(CAST(DATE '2023-01-01' as DATETIME) AS TIMESTAMP))
    AND ab.buy_date < UNIX_SECONDS(CAST(CAST(current_date() as DATETIME) AS TIMESTAMP))
    and _date >= DATE '2022-11-01'
    and v.top_channel in ('us_paid', 'intl_paid')
    and reporting_channel_group = 'Display'
group by 1,2,3
*/
