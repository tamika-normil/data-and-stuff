--- of all past visitors, what is the opt status across their browsers?
--- add GDPR filter from `etsy-data-warehouse-prod.etsy_shard.user_privacy_details`

begin

DECLARE attribution_window_number_of_days ARRAY<int64>;
DECLARE attribution_window_number_of_day INT64;
DECLARE i INT64 DEFAULT 0;

SET attribution_window_number_of_days =  [90,180,365,365*2,365*3];


LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(attribution_window_number_of_days) THEN 
    LEAVE; 
  END IF;

SET attribution_window_number_of_day = attribution_window_number_of_days[ORDINAL(i)];

create or replace temporary table att_visits as (
    select distinct visit_id
    from `etsy-data-warehouse-prod.etsy_aux.appsflyer` a 
    join `etsy-data-warehouse-prod.buyer_growth.native_ids` b on a.ios_advertising_id=b.idfa
    and a.att_status=3 and b.event_source='ios'
    where b._date>=(current_date-attribution_window_number_of_day)
);

if i = 1 then 

create or replace table etsy-data-warehouse-dev.tnormil.opp_sizing_liveramp as
(with base as 
(select mapped_user_id,u.user_id, a.browser_id, a.visit_id, case when mapped_platform_type like 'boe_ios%' and d.visit_id is null then 1 else 0 
end as opt_out
from `etsy-data-warehouse-prod.buyatt_mart.visits` a
left join att_visits d on a.visit_id=d.visit_id
left join etsy-data-warehouse-prod.user_mart.user_profile u on a.user_id = u.user_id
where a._date>=(current_date-attribution_window_number_of_day) and a.run_date>=unix_seconds(timestamp(current_date-attribution_window_number_of_day))),
agg_metrics as 
(select attribution_window_number_of_day, mapped_user_id, count(distinct browser_id) as unique_browsers, count(distinct case when opt_out = 1 then browser_id end) as unique_opt_out_browsers
, count(browser_id) as browsers
, count(case when opt_out = 1 then browser_id end) as opt_out_browsers
, count(visit_id) as visits
, count(case when opt_out = 1 then visit_id end) as opt_out_visits
from base
group by 1,2)
select *
from agg_metrics
order by 2 desc);

else 

insert into etsy-data-warehouse-dev.tnormil.opp_sizing_liveramp
(with base as 
(select mapped_user_id,u.user_id, a.browser_id, a.visit_id, case when mapped_platform_type like 'boe_ios%' and d.visit_id is null then 1 else 0 
end as opt_out
from `etsy-data-warehouse-prod.buyatt_mart.visits` a
left join att_visits d on a.visit_id=d.visit_id
left join etsy-data-warehouse-prod.user_mart.user_profile u on a.user_id = u.user_id
where a._date>=(current_date-attribution_window_number_of_day) and a.run_date>=unix_seconds(timestamp(current_date-attribution_window_number_of_day))),
agg_metrics as 
(select attribution_window_number_of_day, mapped_user_id, count(distinct browser_id) as unique_browsers, count(distinct case when opt_out = 1 then browser_id end) as unique_opt_out_browsers
, count(browser_id) as browsers
, count(case when opt_out = 1 then browser_id end) as opt_out_browsers
, count(visit_id) as visits
, count(distinct case when opt_out = 1 then visit_id end) as opt_out_visits
from base
group by 1,2)
select *
from agg_metrics
order by 2 desc);

end if;

END LOOP;

end;

--- recreate receipt data based on user data we can track 

create or replace temporary table att_visits as (
    select distinct visit_id
    from `etsy-data-warehouse-prod.etsy_aux.appsflyer` a 
    join `etsy-data-warehouse-prod.buyer_growth.native_ids` b on a.ios_advertising_id=b.idfa
    and a.att_status=3 and b.event_source='ios'
    where b._date>=(current_date-1095)
);


CREATE TEMPORARY TABLE user1
  AS WITH opt_in_receipts as 
    (select distinct receipt_id, max(case when mapped_platform_type like 'boe_ios%' and d.visit_id is null then 1 else 0 
end) as opt_out
      from etsy-data-warehouse-prod.transaction_mart.transactions_visits tv
      left join att_visits d using (visit_id)
      group by 1),
  dat AS (
    SELECT
        mapped_user_id,
        ar.receipt_id,
        is_guest_checkout,
        DATE(creation_tsz) AS purchase_date,
        DATE(first_receipt_tsz) AS first_purchase_date,
        row_number() OVER (PARTITION BY mapped_user_id ORDER BY creation_tsz, ar.receipt_id) AS purchase_number --  add receipt_id as tie breaker
      FROM
        `etsy-data-warehouse-prod.transaction_mart.all_receipts` ar
        left join opt_in_receipts o using (receipt_id)
      WHERE 
       opt_out = 0
       AND receipt_live = 1
       AND mapped_user_id <> 46475043
       AND DATE(creation_tsz) < current_date()
    ORDER BY
      receipt_id
  )
  SELECT
      a.mapped_user_id,
      a.receipt_id,
      a.is_guest_checkout,
      a.purchase_date,
      a.first_purchase_date,
      a.purchase_number,
      b.buyer_country_name AS buyer_country,
      CASE
        WHEN b.buyer_country_name <> b.seller_country_name THEN 1
        ELSE 0
      END AS is_international,
      b.gms_net AS receipt_gms
    FROM
      `etsy-data-warehouse-prod.transaction_mart.receipts_gms` AS b
      INNER JOIN dat AS a USING (receipt_id)
    WHERE b.gms_net > 0
;
--  45 sec
CREATE TEMPORARY TABLE user2
  AS  WITH rank AS (
    SELECT
        mapped_user_id,
        purchase_date,
        sum(receipt_gms) AS day_gms,
        row_number() OVER (PARTITION BY mapped_user_id ORDER BY unix_date(purchase_date)) AS purchase_days
      FROM
        user1
      GROUP BY 1, 2
-- removing ORDER BY for memory purposes
--    ORDER BY
--      1,
--      2
  )
  SELECT
      mapped_user_id,
      purchase_date,
      day_gms,
      purchase_days,
      CASE
        WHEN purchase_days = 1 THEN 0
        ELSE date_diff(purchase_date, lag(purchase_date, 1) OVER (PARTITION BY mapped_user_id ORDER BY unix_date(purchase_date)), DAY)
      END AS days_since
    FROM
      rank
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.ltv_user_data`
  AS SELECT
      a.mapped_user_id,
      a.purchase_date,
      --  extract(epoch from a.purchase_date)::int as run_date,
      a.receipt_id,
      a.receipt_gms,
      a.purchase_number,
      b.purchase_days,
      a.is_guest_checkout,
      a.first_purchase_date,
      a.buyer_country,
      b.days_since,
      CASE
        WHEN a.is_international > 0 THEN 1
        ELSE 0
      END AS is_international,
      -- a.receipt_gms / b.day_gms AS receipt_percentage
	  round(cast(a.receipt_gms / b.day_gms as NUMERIC), 3) AS receipt_percentage,
     CASE
          WHEN b.purchase_days = 1 THEN 'new_buyer'
          WHEN b.days_since > 365 THEN 'reactivated_buyer'
          ELSE 'existing_buyer'
        END AS buyer_type
    FROM
      user1 AS a
      INNER JOIN user2 AS b USING (mapped_user_id, purchase_date)
    WHERE a.purchase_date >= DATE '2014-01-01'
;



/*

###future checks
-- can a browser id have multiplier opt out statuses?
-- do the results change if I query for all brower has, not just those with visits?

select 
from etsy-data-warehouse-prod.user_mart.user_profile
left join `etsy-data-warehouse-prod.hvoc.customers_by_browser`

*/

