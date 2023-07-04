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
    traffic_source.name as first_campaign,
    device.category AS device,
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
      (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_location') LIKE '%gBraid%' AS has_gBraid,
       (SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_location') LIKE '%fbclid%' AS has_fbclid,
    CASE
      WHEN event_name = 'page_view' OR event_name = 'screen_view' THEN 1
    ELSE
    0
  END
    AS views,
    CASE
      WHEN event_name = 'sign_up' THEN 1
    ELSE
    0
  END
    AS sign_up,
    CASE
      WHEN event_name = 'add_to_cart' THEN 1
    ELSE
    0
  END
    AS addtocart
  FROM
    {{ source('ga4', 'events') }}),
--   WHERE
--     event_name = 'page_view'
--     OR event_name = 'screen_view'
--     OR event_name = 'sign_up'
--     OR event_name = 'add_to_cart' 

--if durring one session user had two or more sources, CTE takes first one
 basic_sessions_sources AS (
  SELECT
    * EXCEPT(user_id),
     last_value(user_id ignore nulls) over (partition by user_pseudo_id order by event_timestamp rows between unbounded preceding and unbounded following) as user_id,
    FIRST_VALUE(source IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_source,
    FIRST_VALUE(medium IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_medium,
     FIRST_VALUE(campaign IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_campaign
  FROM
    session_sources ),

--fix of the gclid problem in the previous table
--if session_number equals to 1, takes as source, medium and campaign first source, first medium and first campaign
--if page_location includes gclid or wbraid set source as google and medium as cpc
--in other cases takes original source, medium and campaign   
  fix_cpc_session_sources AS (
  SELECT
    date,
    SUM(views) AS views,
    SUM(sign_up) AS sign_up,
    SUM(addtocart) AS addtocart,
    session_id,
    user_pseudo_id,
    user_id,
    CASE
      WHEN session_number = 1 THEN 'new'
    ELSE
    'returning'
  END
    AS user_type,
    first_source,
    first_medium,
    device,
    platform,
    CASE
      WHEN session_number = 1 THEN first_source
      WHEN (has_gclid IS TRUE
      AND first_session_source IS NULL)
    OR (has_wbraid IS TRUE
      AND first_session_source IS NULL)
    OR (has_gBraid IS TRUE
      AND first_session_source IS NULL) THEN 'google'
    WHEN has_fbclid IS TRUE
      AND first_session_source IS NULL THEN 'facebook'
    ELSE
    first_session_source
  END
    AS source,
    CASE
      WHEN session_number = 1 THEN first_medium
      WHEN (has_gclid IS TRUE
      AND first_session_medium IS NULL)
    OR (has_wbraid IS TRUE
      AND first_session_medium IS NULL)
    OR (has_gBraid IS TRUE
      AND first_session_medium IS NULL) THEN 'cpc'
    WHEN has_fbclid IS TRUE
      AND first_session_medium IS NULL THEN 'cpc'
    ELSE
    first_session_medium
  END
    AS medium,
    CASE
      WHEN session_number = 1 THEN first_campaign
      WHEN (has_gclid IS TRUE
      AND first_session_campaign IS NULL)
    OR (has_wbraid IS TRUE
      AND first_session_campaign IS NULL)
    OR (has_gBraid IS TRUE
      AND first_session_campaign IS NULL) THEN '(cpc)'
      WHEN has_fbclid IS TRUE
      AND first_session_campaign IS NULL THEN '(cpc)'
    ELSE
    first_session_campaign
  END
    as campaign,
  FROM
    basic_sessions_sources
  GROUP BY
    date,
    session_id,
    user_pseudo_id,
    user_id,
    user_type,
    first_source,
    first_medium,
    device,
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
    sign_up,
    addtocart,
    session_id,
    user_pseudo_id,
    user_id,
    user_type,
    first_source,
    first_medium,
    device,
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
    `turbo-ukr.analytics_286195171.events_*` ),

  all_page_path_window AS (
  SELECT
    date,
    session_id,
    FIRST_VALUE(page_path) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS page_path,
  FROM
    all_page_path ),

--takes date, distinct session_id and first page location
  first_page_path AS (
  SELECT
    DISTINCT session_id,
    date,
    page_path,
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

ga_all_purchases as (
SELECT
distinct CONCAT(user_pseudo_id, (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id')) AS session_id,  
      ecommerce.transaction_id as transaction_id,
    CAST(PARSE_DATE('%Y%m%d', event_date) AS date) AS date,
    event_timestamp,
    ROW_NUMBER() over (partition by ecommerce.transaction_id order by event_timestamp desc) as row_transaction
    
  FROM
    {{ source('ga4', 'events') }} where ecommerce.transaction_id is not null and ecommerce.transaction_id != '(not set)' and event_name = 'purchase'
),

ga_transactions as (
SELECT
    session_id,  
    transaction_id,
    date   
  FROM
    ga_all_purchases
    WHERE row_transaction = 1
),

--creates final ga table
--joins base table form GA with fixed gclid sources and mediums with tech table and table with first page location
--if page location contains utms and source/medium is null, takes utms source/medium
--adds campaign_id, ad_group, ad_group_id, ad_id from utms
  ga_table AS (
  SELECT
    fix_duplicates.date,
    fix_duplicates.session_id,
    session_start_end.session_start,
    session_start_end.session_end,
    fix_duplicates.user_pseudo_id,
    fix_duplicates.user_id,
    fix_duplicates.user_type,
    first_page_path.page_path,
    CASE
      WHEN fix_duplicates.source IS NULL AND first_page_path.page_path LIKE '%utm_source=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_source=([^&]+).*'),'%5C','\\')
      ELSE
      fix_duplicates.source
    END
      AS source,
      CASE
        WHEN fix_duplicates.medium IS NULL AND first_page_path.page_path LIKE '%utm_medium=%' THEN REPLACE(REGEXP_EXTRACT(first_page_path.page_path, r'.*utm_medium=([^&]+).*'),'%5C','\\')
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
        fix_duplicates.first_source,
        fix_duplicates.first_medium,
        fix_duplicates.device,
        CASE WHEN platform = 'IOS' then 'IOS'
        WHEN platform = 'ANDROID' then 'ANDROID'
        WHEN platform = 'WEB' then CONCAT(UPPER(device), ' ', platform) END as device_platform,
        fix_duplicates.platform,
        ga_transactions.transaction_id,
        SUM(fix_duplicates.views) AS views,
        SUM(fix_duplicates.sign_up) AS sign_up,
        SUM(addtocart) AS addtocart
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
    LEFT JOIN
        ga_transactions
      ON
        fix_duplicates.session_id=ga_transactions.session_id
        AND fix_duplicates.date=ga_transactions.date
      GROUP BY
        fix_duplicates.date,
        fix_duplicates.session_id,
        session_start_end.session_start,
        session_start_end.session_end,
        fix_duplicates.user_pseudo_id,
        fix_duplicates.user_id,
        fix_duplicates.user_type,
        first_page_path.page_path,
        source,
        medium,
        campaign,
        campaign_id,
        ad_group,
        ad_group_id,
        ad_id,
        fix_duplicates.first_source,
        fix_duplicates.first_medium,
        fix_duplicates.device,
        fix_duplicates.platform,
        ga_transactions.transaction_id),

final_ga_table as (
  select date,
    session_id,
    session_start,
    session_end,
    CASE
          WHEN user_id IS NOT NULL then user_id
          -- WHEN user_pseudo_id IS NULL and user_id is NULL and client_id IS NOT NULL THEN client_id
          -- WHEN user_pseudo_id IS NOT NULL and user_id is NULL and client_id IS NULL THEN user_id
        ELSE
        user_pseudo_id
      END as user_pseudo_id,
    user_type,
    page_path,
    source,
    medium,
	campaign,
    campaign_id,
    ad_group,
    ad_group_id,
    ad_id,
    first_source,
    first_medium,
    device,
    device_platform,
    platform,
    transaction_id,
    views,
  sign_up,
  addtocart FROM ga_table

),


crm_revenue AS (
      SELECT
        EXTRACT(date
        FROM
          order_date) AS date,
        order_date,
        UNIX_MICROS(order_date) AS date_time,
        user_id,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        1 AS transactions,
        SUM(value) AS revenue,
        SUM(margin) AS margin
      FROM
         {{ ref("base_deals") }} --
      -- WHERE
      --   DATE(order_date) = '2023-05-01'
      GROUP BY
        date,
        order_date,
        date_time,
        user_id,
        client_id,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status ),

-- get running sum of revenue by user
      crm_revenue_ltv AS (
      SELECT
        *,
        SUM(revenue) OVER (PARTITION BY user_id ORDER BY UNIX_MICROS(order_date)) AS ltv,
        SUM(margin) OVER (PARTITION BY user_id ORDER BY UNIX_MICROS(order_date)) AS ltv_margin,
         COUNT(transaction_id) OVER (PARTITION BY user_id ORDER BY UNIX_MICROS(order_date)) AS all_orders,
       FROM
        crm_revenue ),


        crm_revenue_ga AS (
      SELECT
    distinct crm_revenue_ltv.transaction_id,
        crm_revenue_ltv.date,
        crm_revenue_ltv.date_time,
        crm_revenue_ltv.order_date,
        final_ga_table.session_id,
        final_ga_table.session_start,
        final_ga_table.user_pseudo_id,
        crm_revenue_ltv.user_id,
        final_ga_table.source,
        final_ga_table.medium,
        final_ga_table.campaign,
        final_ga_table.campaign_id,
        final_ga_table.ad_group,
        final_ga_table.ad_group_id,
        final_ga_table.ad_id,
        final_ga_table.first_source,
        final_ga_table.first_medium,
        final_ga_table.device,
        final_ga_table.platform,
        crm_revenue_ltv.payment_method,
        UPPER(crm_revenue_ltv.platform) AS platform_crm,
        crm_revenue_ltv.gender,
        crm_revenue_ltv.birth_date,
        crm_revenue_ltv.status,
        final_ga_table.views,
        final_ga_table.sign_up,
        final_ga_table.addtocart,
        crm_revenue_ltv.transactions,
        crm_revenue_ltv.revenue,
        crm_revenue_ltv.margin,
        crm_revenue_ltv.ltv,
		crm_revenue_ltv.ltv_margin,
        crm_revenue_ltv.all_orders
      FROM
        crm_revenue_ltv
      LEFT JOIN
        final_ga_table
      ON
        crm_revenue_ltv.transaction_id = final_ga_table.transaction_id),
       
crm_revenue_ga_fix_uid AS (
      SELECT
        date,
        date_time,
        session_id ,
        CASE
          WHEN session_start IS NULL THEN order_date
        ELSE
        session_start
      END
        AS session_start,
        order_date,
       CASE
          WHEN user_id IS NOT NULL then user_id
          -- WHEN user_pseudo_id IS NULL and user_id is NULL and client_id IS NOT NULL THEN client_id
          -- WHEN user_pseudo_id IS NOT NULL and user_id is NULL and client_id IS NULL THEN user_id
        ELSE
        user_pseudo_id
      END as user_pseudo_id,
        source,
        medium,
        campaign,
        campaign_id,
       ad_group,
        ad_group_id,
        ad_id,
        first_source,
        first_medium,
        device,
        CASE
          WHEN platform IS NULL THEN platform_crm
        ELSE
        platform END AS platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        views,
        sign_up,
        addtocart,
        transactions,
        revenue,
        margin,
        ltv,
		ltv_margin,
        all_orders
      FROM
        crm_revenue_ga),

--is first order    
      crm_revenue_ga_is_first AS (
      SELECT
        *,
        CASE WHEN order_date = first_value(order_date) over (partition by user_pseudo_id order by order_date) THEN 1
        ELSE 0 END as isFirst
      FROM
        crm_revenue_ga_fix_uid),

--fix empty session_id
      fix_session_id as(
        SELECT
        date,
        CASE WHEN session_id is NULL THEN CONCAT(user_pseudo_id, '.', date_time)
        ELSE
        session_id
        END 
        as session_id,
        session_start,
        order_date,
        user_pseudo_id,
        source,
        medium,
        campaign,
        campaign_id,
       ad_group,
        ad_group_id,
        ad_id,
        first_source,
        first_medium,
        device,
        CASE WHEN platform = 'IOS' then 'IOS'
        WHEN platform = 'ANDROID' then 'ANDROID'
        WHEN platform = 'WEB' then CONCAT(UPPER(device), ' ', platform) END as device_platform,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        CASE WHEN status = 'Success' THEN 1
        ELSE 0 END as status,
        isFirst,
        views,
        sign_up,
        addtocart,
        transactions,
        revenue,
        margin,
        ltv,
		ltv_margin,
        all_orders
      FROM
        crm_revenue_ga_is_first
      ),

      all_sessions_transactions AS(
      SELECT
        date,
        session_id,
        session_start,
        CAST(NULL AS TIMESTAMP) AS order_date,
        user_pseudo_id,
        source,
        medium,
        campaign,
        campaign_id,
       ad_group,
        ad_group_id,
        ad_id,
        first_source,
        first_medium,
        device,
        device_platform,
        platform,
        CAST(NULL AS STRING) AS payment_method,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS birth_date,
        CAST (NULL AS STRING) AS transaction_id,
        null AS status,
        null as isFirst,
        views,
        sign_up,
        addtocart,
        null AS transactions,
        null AS revenue,
        null AS margin,
        null AS ltv,
        null AS ltv_margin,
        null as all_orders
      FROM
        final_ga_table
      WHERE
        views IS NOT NULL
        and 
        session_id not in (SELECT distinct session_id from fix_session_id)
      UNION ALL
      SELECT
        date,
        session_id,
        CASE WHEN session_start is null then order_date
        ELSE session_start END as session_start,
        order_date,
        user_pseudo_id,
        source,
        medium,
        campaign,
        campaign_id,
       ad_group,
        ad_group_id,
        ad_id,
        first_source,
        first_medium,
        device,
        device_platform,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        isFirst,
        views,
        sign_up,
        addtocart,
        transactions,
        revenue,
        margin,
        ltv,
		ltv_margin,
        all_orders
      FROM
        fix_session_id),

--gets product from stg table
products_table AS(
  SELECT
    transaction_id,
    products,
    delivery
  FROM
   {{ source("raw_db", "raw_deals") }}),

--join products to crm data
crm_revenue_agg AS (
  SELECT
  all_sessions_transactions.date,
        all_sessions_transactions.session_id,
        all_sessions_transactions.session_start,
        all_sessions_transactions.order_date,
        all_sessions_transactions.user_pseudo_id,
        all_sessions_transactions.source,
        all_sessions_transactions.medium,
        all_sessions_transactions.campaign,
       all_sessions_transactions.campaign_id,
       all_sessions_transactions.ad_group,
        all_sessions_transactions.ad_group_id,
        all_sessions_transactions.ad_id,
        all_sessions_transactions.first_source,
        all_sessions_transactions.first_medium,
        all_sessions_transactions.device,
        all_sessions_transactions.device_platform,
        all_sessions_transactions.platform,
        all_sessions_transactions.payment_method,
        all_sessions_transactions.gender,
        all_sessions_transactions.birth_date,
        all_sessions_transactions.transaction_id,
        all_sessions_transactions.status,
        all_sessions_transactions.isFirst,
        all_sessions_transactions.views,
        all_sessions_transactions.sign_up,
        all_sessions_transactions.addtocart,
        all_sessions_transactions.transactions,
        all_sessions_transactions.revenue,
        all_sessions_transactions.margin,
        all_sessions_transactions.ltv,
		all_sessions_transactions.ltv_margin,
        all_sessions_transactions.all_orders,
        products_table.products,
        products_table.delivery
  FROM
    all_sessions_transactions
  LEFT JOIN
    products_table
  ON
    all_sessions_transactions.transaction_id = products_table.transaction_id ),

--agregates all data
      all_views_transactions_agg AS (
      SELECT
        session_id,
        date,
        session_start,
        order_date,
        user_pseudo_id,
        CASE
          WHEN UNIX_MICROS(session_start) = FIRST_VALUE(UNIX_MICROS(session_start)) OVER (PARTITION BY user_pseudo_id ORDER BY UNIX_MICROS(session_start)) THEN 'new'
        ELSE
        'returning'
      END
        AS user_type,
        CASE
          WHEN source IS NULL THEN '(direct)'
          WHEN source like '%ipay%' THEN '(direct)'
        ELSE
        source
      END
        AS source,
        CASE
          WHEN medium IS NULL THEN '(none)'
          WHEN source like '%ipay%' THEN '(none)'
        ELSE
        medium
      END
        AS medium,
        CASE
          WHEN campaign IS NULL THEN '(none)'
        ELSE
        campaign
      END
        AS campaign,
        CASE
          WHEN campaign_id IS NULL THEN '(none)'
        ELSE
        campaign_id
      END
        AS campaign_id,
       CASE
          WHEN ad_group IS NULL THEN '(none)'
        ELSE
        ad_group
      END
        AS ad_group,
        CASE
          WHEN ad_group_id IS NULL THEN '(none)'
        ELSE
        ad_group_id
      END
        AS ad_group_id,
        CASE
          WHEN ad_id IS NULL THEN '(none)'
        ELSE
        ad_id
      END
        AS ad_id,
        first_source,
        first_medium,
        device,
        device_platform,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        isFirst,
        products,
        delivery,
        views,
        sign_up,
        addtocart,
        transactions,
        revenue,
        margin,
        ltv,
		ltv_margin,
        all_orders
      FROM
        crm_revenue_agg),

--adds signup to order rate
signup_to_order as (

  SELECT *, CASE WHEN sum(sign_up) over (partition by user_pseudo_id order by session_start ROWS between unbounded preceding and current row ) > 0 THEN 1
  WHEN sum(transactions) over (partition by user_pseudo_id order by session_start ROWS between unbounded preceding and current row) > 0 THEN 1
  ELSE 0 END as isSignup, 
  CASE WHEN sum(transactions) over (partition by user_pseudo_id order by session_start ROWS between unbounded preceding and current row) > 0 THEN 1
  ELSE 0 END as isOrder,
  CASE WHEN CONCAT(source, '/', medium) != '(direct)/(none)' THEN session_start else null END as last_non_direct_time
     FROM all_views_transactions_agg
),

--returns last non direct attribution
      last_non_direct AS (
      SELECT
        date,
        session_id,
        session_start,
        order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        campaign,
        campaign_id,
       ad_group,
        ad_group_id,
        ad_id,
        first_source,
        first_medium,
        device,
        device_platform,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        isFirst,
        isSignup,
        isOrder,
        products,
        delivery,
        views,
        sign_up,
        addtocart,
        ltv,
		ltv_margin,
        all_orders,
        transactions,
        revenue,
        margin,
        CASE
        WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and (transactions > 0 or sign_up > 0 or revenue > 0 or margin > 0) and date_diff(session_start, last_value(last_non_direct_time ignore nulls) over(partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row), day) < 30
        THEN last_value(NULLIF(source, '(direct)') IGNORE NULLS) over (partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row)
        ELSE source END
        AS last_non_direct_source,
CASE
        WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and (transactions > 0 or sign_up > 0 or revenue > 0 or margin > 0) and date_diff(session_start, last_value(last_non_direct_time ignore nulls) over(partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row), day) < 30 THEN last_value(NULLIF(medium, '(none)') IGNORE NULLS) over (partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row)
        ELSE medium END
        AS last_non_direct_medium,
CASE
        WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and (transactions > 0 or sign_up > 0 or revenue > 0 or margin > 0) and date_diff(session_start, last_value(last_non_direct_time ignore nulls) over(partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row), day) < 30 THEN last_value(NULLIF(campaign, '(none)') IGNORE NULLS) over (partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row)
        ELSE campaign END
        AS last_non_direct_campaign,
        CASE
        WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and (transactions > 0 or sign_up > 0 or revenue > 0 or margin > 0) and date_diff(session_start, last_value(last_non_direct_time ignore nulls) over(partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row), day) < 30 THEN last_value(NULLIF(ad_group, '(none)') IGNORE NULLS) over (partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row)
        ELSE ad_group END
        AS last_non_direct_ad_group,
        CASE
         WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and (transactions > 0 or sign_up > 0 or revenue > 0 or margin > 0) and date_diff(session_start, last_value(last_non_direct_time ignore nulls) over(partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row), day) < 30 THEN last_value(NULLIF(ad_id, '(none)') IGNORE NULLS) over (partition by user_pseudo_id ORDER BY session_start ROWS between unbounded preceding and current row)
        ELSE ad_id END
        AS last_non_direct_ad_id
      FROM
        signup_to_order),

--aggregates all to simple table
--returns first session time
      final_table AS (
      SELECT
        date,
        session_id,
        session_start,
        order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        campaign,
        campaign_id,
       ad_group,
        ad_group_id,
        ad_id,
        device,
        device_platform,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        isFirst,
        isSignup,
        isOrder,
        products,
        delivery,
        views,
        addtocart,
        ltv,
        all_orders,
        sign_up,
        transactions,
        revenue,
        margin,
		ltv_margin,
    CASE WHEN last_non_direct_source is null then '(direct)' 
        ELSE last_non_direct_source END as 
        last_non_direct_source,
        CASE WHEN last_non_direct_medium is null then '(none)' 
        ELSE last_non_direct_medium END as last_non_direct_medium,
        last_non_direct_campaign,
        last_non_direct_ad_group,
        last_non_direct_ad_id,
        CASE WHEN first_source is null then '(direct)'
         ELSE first_source END as first_source,
        CASE WHEN first_medium is null then '(none)'
         ELSE first_medium END as first_medium
        from
        last_non_direct)
      

--final query
   SELECT 
    * 
   FROM final_table

