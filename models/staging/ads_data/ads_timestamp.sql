{{ config(materialized='table')}}

WITH

--base table from GA with source, medium, campaign
--takes session_id as user_pseudo_id + session_id 
--unnest events parameters source, medium, campaign
--adds columns with true or false if page_location has gclid or wbraid
--adds sum of screen or page views

  session_sources AS (
  SELECT
    CAST(PARSE_DATE('%Y%m%d', event_date) AS date) AS date,
    CONCAT(user_pseudo_id, (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id')) AS session_id,
    event_timestamp,
    event_name,
    user_pseudo_id,
    user_id,
    traffic_source.source AS first_source,
    traffic_source.medium AS first_medium,
    traffic_source.name AS first_campaign,
    (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'source') AS source,
   (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'medium') AS medium,
    (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'campaign') AS campaign,
   (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_number') AS session_number,
        platform,
   (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location') LIKE '%gclid%' AS has_gclid,
    (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location') LIKE '%wbraid%' AS has_wbraid,
    CASE
        WHEN event_name = 'page_view' OR event_name = 'screen_view' THEN 1
      ELSE
      0
    END
     AS views
  FROM
    {{ source('ga4', 'events') }}
    WHERE event_name = 'page_view' or event_name = 'screen_view'
  ),

--fix of the gclid problem in the previous table
--if session_number equals to 1, takes as source, medium and campaign first source, first medium and first campaign
--if page_location includes gclid or wbraid set source as google and medium as cpc
--in other cases takes original source, medium and campaign


basic_sessions_sources as (
  select *, first_value(source ignore nulls) over (partition by user_pseudo_id, session_id order by event_timestamp rows between unbounded preceding and unbounded following) as first_session_source,
  first_value(medium ignore nulls) over (partition by user_pseudo_id, session_id order by event_timestamp rows between unbounded preceding and unbounded following) as first_session_medium,
  first_value(campaign ignore nulls) over (partition by user_pseudo_id, session_id order by event_timestamp rows between unbounded preceding and unbounded following) as first_session_campaign from session_sources
),

  fix_cpc_session_sources AS (
  SELECT
    date,
    sum(views) as views,
    session_id,
    user_pseudo_id,
    first_source,
    first_medium,
    first_campaign,
    platform,
    CASE
      WHEN session_number = 1 THEN first_source
      WHEN (has_gclid IS TRUE
      AND first_session_source IS NULL)
    OR (has_wbraid IS TRUE
      AND first_session_source IS NULL) THEN 'google'
    ELSE
    first_session_source
  END
    AS source,
    CASE
      WHEN session_number = 1 THEN first_medium
      WHEN (has_gclid IS TRUE
      AND first_session_medium IS NULL)
    OR (has_wbraid IS TRUE
      AND first_session_medium IS NULL) THEN 'cpc'
    ELSE
    first_session_medium
  END
    AS medium,
    CASE
      WHEN session_number = 1 THEN first_campaign
      WHEN (has_gclid IS TRUE
      AND first_session_campaign IS NULL)
    OR (has_wbraid IS TRUE
      AND first_session_campaign IS NULL) THEN '(cpc)'
    ELSE
    first_session_campaign
  END
    AS campaign
  FROM
    basic_sessions_sources
  GROUP BY
  date,
  session_id,
    user_pseudo_id,
    first_source,
    first_medium,
    first_campaign,
    platform,
    source,
    medium,
    campaign),

--agregates same sessions that may have filled and empty source/medium. Window function fill nulls
-- takes sessions that have page_views > 0

  fix_duplicates AS (
  SELECT
    date,
    views,
    session_id,
    user_pseudo_id,
    first_source,
    first_medium,
    first_campaign,
    platform,
    COALESCE(source, LAST_VALUE(source IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY source DESC)) AS source,
    COALESCE(medium, LAST_VALUE(medium IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY medium DESC)) AS medium,
    COALESCE(campaign, LAST_VALUE(campaign IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY campaign DESC)) AS campaign,
  FROM
    fix_cpc_session_sources
  WHERE
    views > 0 ),
  
--creates window by session_id in GA table and takes first page_location

  all_page_path AS (
  SELECT
    CAST(PARSE_DATE('%Y%m%d', event_date) AS date) AS date,
    event_timestamp,
    CONCAT(user_pseudo_id, (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id')) AS session_id,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_location') AS page_path,
  FROM
    {{ source('ga4', 'events') }} ),
  all_page_path_window AS (
  SELECT
    date,
    session_id,
    FIRST_VALUE(page_path) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS page_path
  FROM
    all_page_path ),

--takes date, distinct session_id and first page location

  first_page_path AS (
  SELECT
    DISTINCT session_id,
    date,
    page_path
  FROM
    all_page_path_window ),

--takes session_id, user_pseudo_id, event_timestamp from GA table
  session_start_end_ga AS (
  SELECT
    CONCAT(user_pseudo_id, (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id')) AS session_id,
    CAST(PARSE_DATE('%Y%m%d', event_date) AS date) AS date,
    user_pseudo_id,
    event_timestamp
  FROM
    {{ source('ga4', 'events') }}),

--takes session_id, user_pseudo_id and adds one column with timestamp of the first event as a session start and the second with timestamp of the last evens as session end
  session_start_end_arr AS (
  SELECT
    session_id,
    date,
    user_pseudo_id,
    FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS session_start,
    FIRST_VALUE(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp DESC) AS session_end
  FROM
    session_start_end_ga),

--takes only distinct sessions from the previous CTE
  session_start_end AS (
  SELECT
    date,
    session_id,
    user_pseudo_id,
    MAX(TIMESTAMP_MICROS(session_start)) AS session_start,
    MAX(TIMESTAMP_MICROS(session_end)) AS session_end
  FROM
    session_start_end_arr
  GROUP BY
    date,
    session_id,
    user_pseudo_id),

--creates final ga table
--joins base table form GA with fixed gclid sources and mediums with tech table and table with first page location
--if page location contains utms and source/medium is null, takes utms source/medium
--adds campaign_id, ad_group, ad_group_id, ad_id from utms

  final_ga_table AS (
  SELECT
    fix_duplicates.date,
    fix_duplicates.session_id,
    session_start_end.session_start,
    session_start_end.session_end,
    fix_duplicates.user_pseudo_id,
    first_page_path.page_path,
    CASE
      WHEN fix_duplicates.source is null and first_page_path.page_path LIKE '%utm_source=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_source=([^&]+).*'),'%5C','\\')
      ELSE
      fix_duplicates.source
    END
      AS source,
      CASE
        WHEN fix_duplicates.medium is null and first_page_path.page_path LIKE '%utm_medium=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_medium=([^&]+).*'),'%5C','\\')
        ELSE
        fix_duplicates.medium
      END
        AS medium,
        CASE
          WHEN fix_duplicates.campaign is null and  first_page_path.page_path LIKE '%utm_campaign=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_campaign=([^&]+).*'),'%5C','\\')
          ELSE
          fix_duplicates.campaign
        END
          AS campaign,
          REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]campaign_id=([^&]+).*') AS campaign_id,
          CASE WHEN campaign not like '%Performance Max%' then REPLACE( REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]utm_content=([^&]+).*'), '%5C', '\\' ) ELSE null end AS ad_group,
            REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]adset_id=([^&]+).*') AS ad_group_id,
            REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]utm_ad=([^&]+).*') AS ad_id,
            fix_duplicates.platform,
            SUM(fix_duplicates.views) AS views
          FROM
            fix_duplicates
          LEFT JOIN
            first_page_path
          ON
            fix_duplicates.session_id = first_page_path.session_id
            AND fix_duplicates.date = first_page_path.date
          LEFT JOIN
            session_start_end
          ON
            fix_duplicates.session_id=session_start_end.session_id
            AND fix_duplicates.date=session_start_end.date
          GROUP BY
            fix_duplicates.date,
            fix_duplicates.session_id,
            session_start_end.session_start,
            session_start_end.session_end,
            fix_duplicates.user_pseudo_id,
            first_page_path.page_path,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_id,
            fix_duplicates.platform),

--takes transactions data from the base_deals table
-- if client_id is false fill null

        crm_revenue AS (
          SELECT
            EXTRACT(date
            FROM
              order_date) AS date,
            order_date,
            UNIX_MICROS(order_date) AS date_time,
            user_id,
            CASE
              WHEN client_id = 'false' THEN NULL
            ELSE
            client_id
          END
            AS client_id,
            platform,
            COUNT(transaction_id) AS transactions,
            SUM(value) AS revenue
          FROM
            {{ ref("base_deals") }}
           
          GROUP BY
            date,
            order_date,
            date_time,
            user_id,
            client_id,
            platform ),

--joins final_ga_table with crm_revenue 
--joins with transaction on client_id and if time of the transaction is more then session start time and less then ession end time + 30 minutes

          crm_revenue_ga AS (
          SELECT
            crm_revenue.date,
            crm_revenue.date_time,
            final_ga_table.session_id,
            final_ga_table.session_start,
            final_ga_table.user_pseudo_id,
            crm_revenue.user_id,
            final_ga_table.source,
            final_ga_table.medium,
            final_ga_table.campaign,
            final_ga_table.campaign_id,
            final_ga_table.ad_group,
            final_ga_table.ad_group_id,
            final_ga_table.ad_id,
            final_ga_table.platform,
            UPPER(crm_revenue.platform) as platform_crm,
            SUM(crm_revenue.transactions) AS transactions,
            SUM(crm_revenue.revenue) AS revenue
          FROM
            crm_revenue
          LEFT JOIN
            final_ga_table
          ON
            crm_revenue.client_id = final_ga_table.user_pseudo_id
            AND crm_revenue.date = final_ga_table.date
            AND crm_revenue.order_date >= final_ga_table.session_start
            AND crm_revenue.order_date <= final_ga_table.session_end
          GROUP BY
            crm_revenue.date,
            crm_revenue.date_time,
            final_ga_table.session_id,
            final_ga_table.session_start,
            final_ga_table.user_pseudo_id,
            crm_revenue.user_id,
            final_ga_table.source,
            final_ga_table.medium,
            final_ga_table.campaign,
            final_ga_table.campaign_id,
            final_ga_table.ad_group,
            final_ga_table.ad_group_id,
            final_ga_table.ad_id,
            final_ga_table.platform,
            platform_crm),
          
--if user_pseudo_id in the previous table is null, takes user_id form the crm          
          crm_revenue_ga_fix_uid AS (
          SELECT
            date,
            cast(TIMESTAMP_MICROS(date_time) as datetime) as date_time,
            session_id,
            session_start,
            CASE
              WHEN user_pseudo_id IS NULL THEN user_id
            ELSE
            user_pseudo_id
          END
            AS user_pseudo_id,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            CASE 
              WHEN platform IS NULL THEN platform_crm
            ELSE
            platform
          END
            AS platform,
            transactions,
            revenue
          FROM
            crm_revenue_ga)

SELECT * from crm_revenue_ga_fix_uid