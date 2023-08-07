begin

DECLARE end_date DATE DEFAULT '2023-06-01';
DECLARE start_date DATE DEFAULT DATE_ADD(end_date, INTERVAL -365 DAY);
DECLARE attribution_window_number_of_days ARRAY<int64>;
DECLARE attribution_window_number_of_day INT64;
#DECLARE earliest_attributable_click_date DATE DEFAULT (SELECT DATE_TRUNC(DATE_SUB(start_date, INTERVAL attribution_window_number_of_days DAY), DAY));
DECLARE earliest_attributable_click_date DATE;
DECLARE event_guid_max_length INT64 DEFAULT 64;
DECLARE i INT64 DEFAULT 0;

select start_date, end_date;

CREATE TEMPORARY TABLE purchases AS
  WITH raw_purchases as (
    SELECT
      a.receipt_id,
      a.buyer_user_id,
      a.seller_user_id,
      d.shop_id,
      a.creation_tsz,
      b.visit_id,
      c.browser_id
    FROM
      `etsy-data-warehouse-prod.transaction_mart.all_receipts` AS a
      LEFT OUTER JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` AS b ON a.receipt_id = b.receipt_id
      LEFT OUTER JOIN etsy-data-warehouse-prod.buyatt_mart.visits AS c ON b.visit_id = c.visit_id
      LEFT OUTER JOIN `etsy-data-warehouse-prod.rollups.seller_basics` AS d ON a.seller_user_id = d.user_id
    WHERE DATE(a.creation_tsz)  >= start_date
    AND DATE(a.creation_tsz) <= end_date
    AND DATE(c._date) >= start_date
    AND DATE(c._date) <= end_date
    AND receipt_live=1
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
  SELECT
    distinct p.*,
    COALESCE(cu.customer_id, cg.customer_id, cc.customer_id, '') as customer_id
  FROM raw_purchases p
  LEFT OUTER JOIN `etsy-data-warehouse-prod.hvoc.customers_by_user` AS cu
    ON p.buyer_user_id = cu.user_id
  LEFT OUTER JOIN `etsy-data-warehouse-prod.hvoc.customers_by_guest` AS cg
    ON p.buyer_user_id = cg.guest_id
  LEFT OUTER JOIN `etsy-data-warehouse-prod.hvoc.customers_by_browser` AS cc
    ON p.buyer_user_id = cc.user_id
    AND p.browser_id = cc.browser_id;

select count(1) from purchases;

SET attribution_window_number_of_days =  [1,2,3,7,14,30];

LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(attribution_window_number_of_days) THEN 
    LEAVE; 
  END IF;

SET attribution_window_number_of_day = attribution_window_number_of_days[ORDINAL(i)];

select attribution_window_number_of_day;

SET earliest_attributable_click_date =  (SELECT DATE_TRUNC(DATE_SUB(start_date, INTERVAL attribution_window_number_of_day DAY), DAY));

select earliest_attributable_click_date ;

#SELECT STRUCT<DATE>((SELECT DATE_TRUNC(DATE_SUB(start_date, INTERVAL field DAY), DAY))).*;

create or replace temporary table timely_clicks as
(select * from `etsy-data-warehouse-prod.etsy_shard.ads_attribution_clicks` a
where extract(date from timestamp_seconds(a.click_date)) >= earliest_attributable_click_date);

select count(1) from timely_clicks;

create or replace TEMPORARY TABLE raw_diff_shop_attributions AS
  SELECT a.receipt_id,
      a.browser_id AS buyer_browser_id,
      a.buyer_user_id,
      a.seller_user_id,
      b.channel,
      b.click_date,
      UNIX_SECONDS(a.creation_tsz) AS purchase_date_seconds,
      EXTRACT(DATE FROM a.creation_tsz) AS purchase_date_trunc,
      b.listing_id AS clicked_listing_id,
      b.browser_id AS click_browser_id,
      b.user_id AS click_user_id,
      b.shop_id,
      b.external_click_id,
      b.click_event_guid AS click_guid,
      b.url AS click_url,
      '' AS customer_id,
      'indirect' as type,
  FROM purchases AS a
  INNER JOIN timely_clicks AS b 
  ON a.buyer_user_id = b.user_id
  AND a.shop_id != b.shop_id
  AND a.creation_tsz <= TIMESTAMP_ADD(TIMESTAMP_SECONDS(b.click_date),  interval attribution_window_number_of_day DAY) 
  AND a.creation_tsz > TIMESTAMP_SECONDS(b.click_date)
  UNION ALL
  SELECT
      a_0.receipt_id,
      a_0.browser_id AS buyer_browser_id,
      a_0.buyer_user_id,
      a_0.seller_user_id,
      b_0.channel,
      b_0.click_date,
      UNIX_SECONDS(a_0.creation_tsz) AS purchase_date_seconds,
      EXTRACT(DATE FROM a_0.creation_tsz) AS purchase_date_trunc,
      b_0.listing_id AS clicked_listing_id,
      b_0.browser_id AS click_browser_id,
      b_0.user_id AS click_user_id,
      b_0.shop_id,
      b_0.external_click_id,
      b_0.click_event_guid AS click_guid,
      b_0.url AS click_url,
      '' AS customer_id,
      'indirect' as type,
  FROM purchases AS a_0
  INNER JOIN timely_clicks AS b_0 ON a_0.browser_id = b_0.browser_id
  AND a_0.shop_id != b_0.shop_id
  AND a_0.creation_tsz <= TIMESTAMP_ADD(TIMESTAMP_SECONDS(b_0.click_date), interval attribution_window_number_of_day DAY) 
  AND a_0.creation_tsz > TIMESTAMP_SECONDS(b_0.click_date)
  UNION ALL
  SELECT
      a_1.receipt_id,
      a_1.browser_id AS buyer_browser_id,
      a_1.buyer_user_id,
      a_1.seller_user_id,
      b_1.channel,
      b_1.click_date,
      UNIX_SECONDS(a_1.creation_tsz) AS purchase_date_seconds,
      EXTRACT(DATE FROM a_1.creation_tsz) AS purchase_date_trunc,
      b_1.listing_id AS clicked_listing_id,
      b_1.browser_id AS click_browser_id,
      b_1.user_id AS click_user_id,
      b_1.shop_id,
      b_1.external_click_id,
      b_1.click_event_guid AS click_guid,
      b_1.url AS click_url,
      a_1.customer_id,
      'indirect' as type,
  FROM purchases AS a_1
  INNER JOIN timely_clicks AS b_1 
    ON a_1.customer_id <> '' 
    AND a_1.customer_id = b_1.customer_id
  AND a_1.shop_id != b_1.shop_id
  AND a_1.creation_tsz <= TIMESTAMP_ADD(TIMESTAMP_SECONDS(b_1.click_date), interval attribution_window_number_of_day DAY) 
  AND a_1.creation_tsz > TIMESTAMP_SECONDS(b_1.click_date)
  UNION ALL
   SELECT a.receipt_id,
      a.browser_id AS buyer_browser_id,
      a.buyer_user_id,
      a.seller_user_id,
      b.channel,
      b.click_date,
      UNIX_SECONDS(a.creation_tsz) AS purchase_date_seconds,
      EXTRACT(DATE FROM a.creation_tsz) AS purchase_date_trunc,
      b.listing_id AS clicked_listing_id,
      b.browser_id AS click_browser_id,
      b.user_id AS click_user_id,
      b.shop_id,
      b.external_click_id,
      b.click_event_guid AS click_guid,
      b.url AS click_url,
      '' AS customer_id,
      'direct' as type,
  FROM purchases AS a
  INNER JOIN timely_clicks AS b 
  ON a.buyer_user_id = b.user_id
  AND a.shop_id = b.shop_id
  AND a.creation_tsz <= TIMESTAMP_ADD(TIMESTAMP_SECONDS(b.click_date),  interval attribution_window_number_of_day DAY) 
  AND a.creation_tsz > TIMESTAMP_SECONDS(b.click_date)
  UNION ALL
  SELECT
      a_0.receipt_id,
      a_0.browser_id AS buyer_browser_id,
      a_0.buyer_user_id,
      a_0.seller_user_id,
      b_0.channel,
      b_0.click_date,
      UNIX_SECONDS(a_0.creation_tsz) AS purchase_date_seconds,
      EXTRACT(DATE FROM a_0.creation_tsz) AS purchase_date_trunc,
      b_0.listing_id AS clicked_listing_id,
      b_0.browser_id AS click_browser_id,
      b_0.user_id AS click_user_id,
      b_0.shop_id,
      b_0.external_click_id,
      b_0.click_event_guid AS click_guid,
      b_0.url AS click_url,
      '' AS customer_id,
      'direct' as type,
  FROM purchases AS a_0
  INNER JOIN timely_clicks AS b_0 ON a_0.browser_id = b_0.browser_id
  AND a_0.shop_id = b_0.shop_id
  AND a_0.creation_tsz <= TIMESTAMP_ADD(TIMESTAMP_SECONDS(b_0.click_date), interval attribution_window_number_of_day DAY) 
  AND a_0.creation_tsz > TIMESTAMP_SECONDS(b_0.click_date)
  UNION ALL
  SELECT
      a_1.receipt_id,
      a_1.browser_id AS buyer_browser_id,
      a_1.buyer_user_id,
      a_1.seller_user_id,
      b_1.channel,
      b_1.click_date,
      UNIX_SECONDS(a_1.creation_tsz) AS purchase_date_seconds,
      EXTRACT(DATE FROM a_1.creation_tsz) AS purchase_date_trunc,
      b_1.listing_id AS clicked_listing_id,
      b_1.browser_id AS click_browser_id,
      b_1.user_id AS click_user_id,
      b_1.shop_id,
      b_1.external_click_id,
      b_1.click_event_guid AS click_guid,
      b_1.url AS click_url,
      a_1.customer_id,
      'direct' as type,
  FROM purchases AS a_1
  INNER JOIN timely_clicks AS b_1 
    ON a_1.customer_id <> '' 
    AND a_1.customer_id = b_1.customer_id
  AND a_1.shop_id = b_1.shop_id
  AND a_1.creation_tsz <= TIMESTAMP_ADD(TIMESTAMP_SECONDS(b_1.click_date), interval attribution_window_number_of_day DAY) 
  AND a_1.creation_tsz > TIMESTAMP_SECONDS(b_1.click_date);

select count(1) from raw_diff_shop_attributions;

create or replace TEMPORARY TABLE ranked_diff_shop_attributions 
  AS SELECT *, row_number() OVER (PARTITION BY receipt_id ORDER BY click_date DESC) AS rank
  FROM raw_diff_shop_attributions; 

create or replace temporary table  diff_shop_attributions as
  SELECT receipt_id,
    buyer_browser_id,
    buyer_user_id,
    seller_user_id,
    channel,
    click_date,
    purchase_date_seconds,
    purchase_date_trunc,
    clicked_listing_id,
    click_browser_id,
    click_user_id,
    shop_id,
    external_click_id,
    click_guid,
    click_url,
    customer_id,
    type,
  FROM ranked_diff_shop_attributions WHERE rank = 1;

select count(1) from diff_shop_attributions;

/*
create temporary table incremental_diff_shop_attributions as 
select * from diff_shop_attributions d
where not exists(
  select 1 from `etsy-data-warehouse-prod.etsy_shard.ads_attributed_receipts` a
  where d.receipt_id = a.receipt_id
);
*/

---select count(1) from incremental_diff_shop_attributions;

create or replace temporary table opted_out_shops as 
SELECT
opt1.shop_id
FROM
`etsy-data-warehouse-prod.etsy_shard.off_etsy_ads_opt_out` AS opt1
INNER JOIN (
  SELECT
      shop_id,
      max(update_date) AS update_date
    FROM
      `etsy-data-warehouse-prod.etsy_shard.off_etsy_ads_opt_out`
    GROUP BY 1
) AS max_dates ON max_dates.shop_id = opt1.shop_id
  AND opt1.update_date = max_dates.update_date
WHERE opt1.status IN(
0, 2
);

###--select count(1) from incremental_diff_shop_attributions

select count(1) from opted_out_shops;

/*
create temporary table opted_in_incremental_diff_shop_attributions as
select * from
incremental_diff_shop_attributions i
where i.shop_id not in
(select o.shop_id from opted_out_shops o);
*/

if i = 1 then 

create or replace table etsy-data-warehouse-dev.tnormil.opted_in_incremental_diff_shop_attributions2  as
select i.*,o.gms_net, attribution_window_number_of_day as attribution_window from
diff_shop_attributions i
left join etsy-data-warehouse-prod.transaction_mart.receipts_gms o on i.receipt_id = o.receipt_id
where i.shop_id not in
(select o.shop_id from opted_out_shops o);

else 
insert into etsy-data-warehouse-dev.tnormil.opted_in_incremental_diff_shop_attributions2 
select i.*,o.gms_net, attribution_window_number_of_day as attribution_window from
diff_shop_attributions i
left join etsy-data-warehouse-prod.transaction_mart.receipts_gms o on i.receipt_id = o.receipt_id
where i.shop_id not in
(select o.shop_id from opted_out_shops o);

end if;

END LOOP;

/*
select count(1) from opted_in_incremental_diff_shop_attributions;

--select count(1) from incremental_diff_shop_attributions

create temporary table opted_in_incremental_diff_shop_with_total as
select a.receipt_id, gms_net
from etsy-data-warehouse-prod.transaction_mart.receipts_gms a
join opted_in_incremental_diff_shop_attributions o
on a.receipt_id = o.receipt_id;

--select count(1) from incremental_diff_shop_attributions

select count(1), sum(gms_net) as total_dollars, (sum(gms_net) * 0.12) as twelve_percent_fees from opted_in_incremental_diff_shop_with_total;
*/

end
