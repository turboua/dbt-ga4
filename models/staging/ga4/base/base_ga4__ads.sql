{{ config(materialized="table") }}

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
    user_pseudo_id,
    user_id,
    traffic_source.source AS first_source,
    traffic_source.medium AS first_medium,
    traffic_source.name AS first_campaign,
    MAX((
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'source')) AS source,
    MAX((
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'medium')) AS medium,
    MAX((
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'campaign')) AS campaign,
    MAX((
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_number')) AS session_number,
    MAX((
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location') LIKE '%gclid%') AS has_gclid,
    MAX((
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location') LIKE '%wbraid%') AS has_wbraid,
    SUM(CASE
        WHEN event_name = 'page_view' OR event_name = 'screen_view' THEN 1
      ELSE
      0
    END
      ) AS views
  FROM
    {{ source("ga4", "events") }}
  GROUP BY
    date,
    session_id,
    user_pseudo_id,
    user_id,
    first_source,
    first_medium,
    first_campaign),

--fix of the gclid problem in the previous table
--if session_number equals to 1, takes as source, medium and campaign first source, first medium and first campaign
--if page_location includes gclid or wbraid set source as google and medium as cpc
--in other cases takes original source, medium and campaign

  fix_cpc_session_sources AS (
  SELECT
    date,
    views,
    session_id,
    user_pseudo_id,
    first_source,
    first_medium,
    first_campaign,
    CASE
      WHEN session_number = 1 THEN first_source
      WHEN (has_gclid IS TRUE
      AND source IS NULL)
    OR (has_wbraid IS TRUE
      AND source IS NULL) THEN 'google'
    ELSE
    source
  END
    AS source,
    CASE
      WHEN session_number = 1 THEN first_medium
      WHEN (has_gclid IS TRUE
      AND medium IS NULL)
    OR (has_wbraid IS TRUE
      AND medium IS NULL) THEN 'cpc'
    ELSE
    medium
  END
    AS medium,
    CASE
      WHEN session_number = 1 THEN first_campaign
      WHEN (has_gclid IS TRUE
      AND campaign IS NULL)
    OR (has_wbraid IS TRUE
      AND campaign IS NULL) THEN '(cpc)'
    ELSE
    campaign
  END
    AS campaign
  FROM
    session_sources),

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
    COALESCE(source, LAST_VALUE(source IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY source DESC)) AS source,
    COALESCE(medium, LAST_VALUE(medium IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY medium DESC)) AS medium,
    COALESCE(campaign, LAST_VALUE(campaign IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY campaign DESC)) AS campaign,
  FROM
    fix_cpc_session_sources
  WHERE
    views > 0 ),

--base table from GA with tech parameters like device, platform, browser, etc.
--takes session_id as user_pseudo_id + session_id 

  tech AS (
  SELECT
    CAST(PARSE_DATE('%Y%m%d', event_date) AS date) AS date,
    CONCAT(user_pseudo_id, (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id')) AS session_id,
    platform,
    device.category AS device,
    device.operating_system AS operating_system,
    device.operating_system_version AS operating_system_version,
    device.web_info.browser AS browser,
  FROM
    {{ source("ga4", "events") }}
  GROUP BY
    date,
    session_id,
    platform,
    device,
    operating_system,
    operating_system_version,
    browser ),
  
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
    {{ source("ga4", "events") }} ),
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
    {{ source("ga4", "events") }}),

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
    tech.session_id,
    session_start_end.session_start,
    session_start_end.session_end,
    fix_duplicates.user_pseudo_id,
    first_page_path.page_path,
    CASE
      WHEN first_page_path.page_path LIKE '%utm_source=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_source=([^&]+).*'),'%5C','\\')
      ELSE
      fix_duplicates.source
    END
      AS source,
      CASE
        WHEN first_page_path.page_path LIKE '%utm_medium=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_medium=([^&]+).*'),'%5C','\\')
        ELSE
        fix_duplicates.medium
      END
        AS medium,
        CASE
          WHEN first_page_path.page_path LIKE '%utm_campaign=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_campaign=([^&]+).*'),'%5C','\\')
          ELSE
          fix_duplicates.campaign
        END
          AS campaign,
          REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]campaign_id=([^&]+).*') AS campaign_id,
          REPLACE( REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]utm_content=([^&]+).*'), '%5C', '\\' ) AS ad_group,
            REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]adset_id=([^&]+).*') AS ad_group_id,
            REGEXP_EXTRACT(first_page_path.page_path, r'.*[&?]utm_ad=([^&]+).*') AS ad_id,
            tech.platform,
            tech.device,
            tech.operating_system,
            tech.operating_system_version,
            tech.browser,
            SUM(fix_duplicates.views) AS views
          FROM
            tech
          LEFT JOIN
            fix_duplicates
          ON
            tech.session_id = fix_duplicates.session_id
            AND tech.date = fix_duplicates.date
          LEFT JOIN
            first_page_path
          ON
            tech.session_id = first_page_path.session_id
            AND tech.date = first_page_path.date
          LEFT JOIN
            session_start_end
          ON
            tech.session_id=session_start_end.session_id
            AND tech.date=session_start_end.date
          GROUP BY
            fix_duplicates.date,
            tech.session_id,
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
            tech.platform,
            tech.device,
            tech.operating_system,
            tech.operating_system_version,
            tech.browser),

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
            final_ga_table.device,
            final_ga_table.operating_system,
            final_ga_table.operating_system_version,
            final_ga_table.browser,
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
            platform_crm,
            final_ga_table.device,
            final_ga_table.operating_system,
            final_ga_table.operating_system_version,
            final_ga_table.browser ),
          
--if user_pseudo_id in the previous table is null, takes user_id form the crm          
          crm_revenue_ga_fix_uid AS (
          SELECT
            date,
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
            device,
            operating_system,
            operating_system_version,
            browser,
            transactions,
            revenue
          FROM
            crm_revenue_ga),

--makes UNION ALL with final_ga_table to get all user activity
          all_sessions_transactions AS(
          SELECT
            date,
            session_id,
            session_start,
            user_pseudo_id,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser,
            views,
            0 AS transactions,
            0 AS revenue
          FROM
            final_ga_table
          WHERE views IS NOT NULL
          UNION ALL
          SELECT
            date,
            session_id,
            session_start,
            user_pseudo_id,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser,
            0 AS views,
            transactions,
            revenue
          FROM
            crm_revenue_ga_fix_uid ),

--aggregates union all table and replaces nulls in sources as direct           
          all_views_transactions_agg AS (
          SELECT
            date,
            session_id,
            session_start,
            user_pseudo_id,
            CASE
              WHEN source IS NULL THEN '(direct)'
            ELSE
            source
          END
            AS source,
            CASE
              WHEN medium IS NULL THEN '(none)'
            ELSE
            medium
          END
            AS medium,
            CASE
              WHEN campaign IS NULL THEN '(direct)'
            ELSE
            campaign
          END
            AS campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser,
            SUM(views) AS views,
            SUM(transactions) AS transactions,
            SUM(revenue) AS revenue
          FROM
            all_sessions_transactions
          GROUP BY
            date,
            user_pseudo_id,
            session_id,
            session_start,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser ),

last_non_direct_agg AS (
          SELECT
            date,
            session_id,
            session_start,
            user_pseudo_id,
            source,
            medium,
            COALESCE(
      MAX(CASE WHEN source != '(direct)' AND medium != '(none)' THEN source END) OVER (PARTITION BY user_pseudo_id ORDER BY session_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      '(direct)'
    ) AS last_non_direct_source,
    COALESCE(
      MAX(CASE WHEN source != '(direct)' AND medium != '(none)' THEN medium END) OVER (PARTITION BY user_pseudo_id ORDER BY session_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      '(none)'
    ) AS last_non_direct_medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser,
            views,
            transactions,
            revenue
          FROM
            all_views_transactions_agg),

last_non_direct AS (
          SELECT
            date,
            session_id,
            session_start,
            user_pseudo_id,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser,
            views,
            CASE WHEN source = last_non_direct_source AND medium = last_non_direct_medium THEN transactions ELSE 0 END AS transactions,
  CASE WHEN source = last_non_direct_source AND medium = last_non_direct_medium THEN revenue ELSE 0 END AS revenue
          FROM
            last_non_direct_agg),

final_table as (
  SELECT 
  date,
            
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser,
            sum(views) as views,
            sum(transactions) as transactions,
            sum(revenue) as revenue
            FROM last_non_direct
            GROUP BY 
            date,
            source,
            medium,
            campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id,
            platform,
            device,
            operating_system,
            operating_system_version,
            browser
)


SELECT * FROM final_table where views is not null