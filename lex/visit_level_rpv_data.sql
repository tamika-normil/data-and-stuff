-- GET VISIT LEVEL DATA 

begin

DECLARE config_flag_param STRING; -- DEFAULT "core_buyer_listing_web.elp_simplified_anchor_listing";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify a custom date range, you can also specify the start and end date manually.
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";

-- By default, this script automatically detects whether the experiment is event filtered or not
-- and provides the associated analysis. However, in the case that we want to examine non-filtered
-- results for an event filtered experiment, this variable may be manually set to "FALSE".
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;

-- Generally, this variable should not be overridden, as the grain of analysis should match the
-- bucketing ID type.
DECLARE bucketing_id_type INT64;

drop table if exists `etsy-data-warehouse-dev.tnormil.lex_visit_level_exp`;

create temp table past_experiments as 
    (with get_past_experiments as 
      ( select Catapult_URL as report_link, SUBSTR(Catapult_URL, STRPOS(Catapult_URL, 'catapult/') + length('catapult/'), length(Catapult_URL)) as launch_id
        from etsy-data-warehouse-dev.tnormil.lex_past_experiements),
    get_current_experiments as 
      ( select Catapult_or_Looker as report_link, SUBSTR(Catapult_or_Looker, STRPOS(Catapult_or_Looker, 'catapult/') + length('catapult/'), length(Catapult_or_Looker)) as launch_id
        from etsy-data-warehouse-dev.tnormil.lex_experiments_2025),
    all_exp as 
      (select *
      from get_past_experiments 
      where report_link is not null
      union all
      select *
      from get_current_experiments
      where report_link is not null)
    select a.*, b.config_flag
    from all_exp a
    left join `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` b on a.launch_id = cast(b.launch_id as string));

FOR record IN
  (SELECT distinct config_flag
   from past_experiments
   where config_flag is not null)

DO

Set config_flag_param = record.config_flag;

select distinct config_flag_param from `etsy-data-warehouse-prod.etsy_atlas.catapult_gms_reports` ;

--IF start_date IS NULL OR end_date IS NULL THEN
    SET (start_date, end_date) = (
        SELECT AS STRUCT
            MAX(DATE(boundary_start_ts)) AS start_date,
            MAX(_date) AS end_date,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            experiment_id = config_flag_param
    );
--END IF;

--IF is_event_filtered IS NULL THEN
    SET (is_event_filtered, bucketing_id_type) = (
        SELECT AS STRUCT
            is_filtered,
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
-- ELSE
--     SET bucketing_id_type = (
--         SELECT
--             bucketing_id_type,
--         FROM
--             `etsy-data-warehouse-prod.catapult_unified.experiment`
--         WHERE
--             _date = end_date
--             AND experiment_id = config_flag_param
--     );
-- END IF;

-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.ab_first_bucket` AS (
    SELECT
        bucketing_id,
        bucketing_id_type AS bucketing_id_type,
        variant_id,
        config_flag_param,
        MIN(bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing`
    WHERE
        _date BETWEEN start_date AND end_date
        AND experiment_id = config_flag_param
    GROUP BY
        bucketing_id, bucketing_id_type, variant_id,config_flag_param
);

-- For event filtered experiments, the effective bucketing event for a bucketed unit
-- into a variant is the FIRST filtering event to occur after that bucketed unit was
-- bucketed into that variant of the experiment.
IF is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.config_flag_param,
            MIN(f.event_ts) AS bucketing_ts,
        FROM
            `etsy-data-warehouse-dev.tnormil.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            USING(bucketing_id)
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
            AND f.event_ts >= f.boundary_start_ts
            AND f.event_ts >= a.bucketing_ts
        GROUP BY
            bucketing_id, bucketing_id_type, variant_id,config_flag_param
    );
END IF;

-------------------------------------------------------------------------------------------
-- VISIT IDS TO JOIN WITH EXTERNAL TABLES
-------------------------------------------------------------------------------------------
-- Need visit ids to join with non-Catapult tables?
-- No problem! Here are some examples for how to get the visit ids for each experimental unit.

-- All associated IDs in the bucketing visit
IF NOT is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            a.config_flag_param,
            start_date,
            end_date,
            is_event_filtered,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.tnormil.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.bucketing` b
            USING(bucketing_id, variant_id, bucketing_ts)
        WHERE
            b._date BETWEEN start_date AND end_date
            AND b.experiment_id = config_flag_param
    );
ELSE
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.tnormil.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            a.config_flag_param,
            start_date,
            end_date,
            is_event_filtered,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.tnormil.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            ON a.bucketing_id = f.bucketing_id
            AND a.bucketing_ts = f.event_ts
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
    );
END IF;

CREATE TABLE IF NOT EXISTS `etsy-data-warehouse-dev.tnormil.lex_visit_level_exp`
LIKE `etsy-data-warehouse-dev.tnormil.ab_first_bucket`;

insert into `etsy-data-warehouse-dev.tnormil.lex_visit_level_exp`
select * from  `etsy-data-warehouse-dev.tnormil.ab_first_bucket`;

END FOR;

END
