-- Temporary table creation for first names, last names, and addresses based on user card transactions
CREATE OR REPLACE TEMP TABLE first_last_names_addresses AS 
WITH
  CleanedCardDetails AS (
    -- Filter and transform the card transaction details for valid cards only
    SELECT 
       cc.reference_id,
      LOWER(TRIM(cc.name_on_card)) AS name_on_card,
    FROM 
      `etsy-data-warehouse-prod.etsy_payments.cc_txns` cc
    WHERE 
      LOWER(TRIM(cc.name_on_card)) NOT IN ('i am a giftcard', 'visa', 'mastercard', 'amex', 'american express', 'discover')
      AND TRIM(cc.name_on_card) IS NOT NULL
      AND TRIM(cc.name_on_card) != ''
  ),
  
  ReceiptDetails AS (
    -- Select and count receipt details joined with shop payments
    SELECT 
      r.mapped_user_id,
      COUNT(DISTINCT r.receipt_id) AS receipts,
      cc.name_on_card
    FROM 
      CleanedCardDetails cc
    JOIN 
      `etsy-data-warehouse-prod.etsy_payments.shop_payments` sp ON cc.reference_id = sp.group_payment_id
    JOIN 
      `etsy-data-warehouse-prod.transaction_mart.all_receipts` r ON sp.receipt_id = r.receipt_id
    GROUP BY 
      r.mapped_user_id, cc.name_on_card
  ),
  
  RankedUsers AS (
    -- Rank users by the number of receipts to get the most common transaction details
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY mapped_user_id ORDER BY receipts DESC) AS cc_rank
    FROM 
      ReceiptDetails
  ),
  
  SelectedUserDetails AS (
  -- Filter for the top-ranked user details
  SELECT 
    mapped_user_id,
    name_on_card,
    FIRST_VALUE(SPLIT(name_on_card, ' ')[SAFE_OFFSET(0)]) OVER (PARTITION BY mapped_user_id ORDER BY receipts DESC) AS first_name,
    ARRAY_REVERSE(SPLIT(name_on_card, ' '))[SAFE_OFFSET(0)] AS last_name,
  FROM 
    RankedUsers
  WHERE 
    cc_rank = 1
)
  
-- Final selection and join with user mapping for email hash
SELECT 
  um.user_id,
  TO_HEX(SHA256(TRIM(LOWER(um.primary_email)))) AS hashed_email,
  sd.first_name,
  sd.last_name,
FROM 
  SelectedUserDetails sd
JOIN 
  `etsy-data-warehouse-prod.user_mart.user_mapping` um ON sd.mapped_user_id = um.mapped_user_id
;

CREATE OR REPLACE TEMPORARY TABLE buyer_basics AS (
    SELECT  
          um.user_id as user_id,
          TO_HEX(SHA256(LOWER(um.primary_email))) AS hashed_email
      FROM  
  `etsy-data-warehouse-prod.rollups.buyer_basics`  bas
  JOIN `etsy-data-warehouse-prod.user_mart.user_mapping` as um
    ON um.mapped_user_id = bas.mapped_user_id
);

CREATE OR REPLACE TEMPORARY TABLE selected_audiece as 
    (select b.user_id
    FROM `etsy-data-warehouse-prod.rollups.buyer_attributes_daily_snapshot` b
    -- left join  `etsy-data-warehouse-prod.rollups.seller_basics_all` s using (user_id)
    where dbmarket_buyer_segment in ("High Potential","Repeat","Habitual")  
    -- and s.user_id is null
    union distinct 
    select distinct user_id
    FROM `etsy-data-warehouse-prod.visit_mart.visits` a
    -- left join  `etsy-data-warehouse-prod.rollups.seller_basics_all` s using (user_id)
    where engaged_visit_5mins = 1
    and _date >= date_sub(current_date, interval 365 day)
    and a.user_id is not null
    -- and s.user_id is null
    and a.user_id <> 0);

CREATE OR REPLACE TEMPORARY TABLE emailable_audiece as (
      select 
        distinct b.user_id
      from 
        `etsy-data-warehouse-prod.rollups.buyer_email_basics_all_vw` a
      join 
        `etsy-data-warehouse-prod.user_mart.user_mapping` b 
      on 
        a.SubscriberKey=b.user_id
      where 
        a.is_new_at_etsy = 1
        and coalesce(a.is_confirmed,0) = 1
        and coalesce(a.is_frozen,0) = 0
        and coalesce(a.is_guest,0) = 0
    );

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.rollups.ciq_buyers` AS 
  SELECT DISTINCT
        bads.canonical_country as country
    ,   NULLIF(COALESCE(bb.hashed_email, ''), '')                           as hashed_email
    ,   NULLIF(COALESCE(flna.first_name, bads.first_name, ''), '')          as first_name
FROM `etsy-data-warehouse-prod.rollups.buyer_attributes_daily_snapshot`     as bads
join selected_audiece as s on bads.user_id = s.user_id
join emailable_audiece as e on bads.user_id = e.user_id
LEFT JOIN first_last_names_addresses flna
  ON flna.user_id = bads.user_id
LEFT JOIN buyer_basics bb
  ON bb.user_id = bads.user_id
WHERE bads.user_id is not null
      and is_seller = 0
      and canonical_country is not null
      and canonical_country not in ('IL','PS')
      --and bads.gdpr_third_party_integration_allowed = 1
      --and bads.att_opt_in = 1
      and not ( NULLIF(bb.hashed_email, '') is null);

-- We need email, first name, and country. 
-- ppl need to be emailable 
-- ./google-cloud-sdk/bin/gcloud init
