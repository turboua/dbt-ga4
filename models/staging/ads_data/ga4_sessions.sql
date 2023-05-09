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
    {{ source('ga4', 'events') }}
  WHERE
    event_name = 'page_view'
    OR event_name = 'screen_view'
    OR event_name = 'sign_up'
    OR event_name = 'add_to_cart' ),

--if durring one session user had two or more sources, CTE takes first one
  basic_sessions_sources AS (
  SELECT
    *,
    FIRST_VALUE(source IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_source,
    FIRST_VALUE(medium IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_medium
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
    CASE
      WHEN session_number = 1 THEN 'new'
    ELSE
    'returning'
  END
    AS user_type,
    first_source,
    first_medium,
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
  FROM
    basic_sessions_sources
  GROUP BY
    date,
    session_id,
    user_pseudo_id,
    user_type,
    first_source,
    first_medium,
    platform,
    source,
    medium),

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
    user_type,
    first_source,
    first_medium,
    platform,
    COALESCE(source, LAST_VALUE(source IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY source DESC)) AS source,
    COALESCE(medium, LAST_VALUE(medium IGNORE NULLS) OVER (PARTITION BY session_id ORDER BY medium DESC)) AS medium,
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
        fix_duplicates.first_source,
        fix_duplicates.first_medium,
        fix_duplicates.platform,
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
      GROUP BY
        fix_duplicates.date,
        fix_duplicates.session_id,
        session_start_end.session_start,
        session_start_end.session_end,
        fix_duplicates.user_pseudo_id,
        fix_duplicates.user_type,
        first_page_path.page_path,
        source,
        medium,
        fix_duplicates.first_source,
        fix_duplicates.first_medium,
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
          WHEN UNIX_MICROS(order_date) = FIRST_VALUE(UNIX_MICROS(order_date)) OVER (PARTITION BY user_id ORDER BY UNIX_MICROS(order_date)) THEN 'new'
        ELSE
        'returning'
      END
        AS user_type_crm,
        CASE
          WHEN client_id = 'false' THEN NULL
        ELSE
        client_id
      END
        AS client_id,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        COUNT(transaction_id) AS transactions,
        SUM(value) AS revenue,
        SUM(margin) AS margin
      FROM
        {{ ref("base_deals") }} 
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
         COUNT(transaction_id) OVER (PARTITION BY user_id ORDER BY UNIX_MICROS(order_date)) AS all_orders,
       FROM
        crm_revenue ),

--joins final_ga_table with crm_revenue 
--joins with transaction on client_id and if time of the transaction is more then session start time and less then session end time    
      crm_revenue_ga AS (
      SELECT
        crm_revenue_ltv.date,
        crm_revenue_ltv.date_time,
        crm_revenue_ltv.order_date,
        final_ga_table.session_id,
        final_ga_table.session_start,
        final_ga_table.user_pseudo_id,
        crm_revenue_ltv.user_id,
        crm_revenue_ltv.client_id,
        final_ga_table.user_type,
        crm_revenue_ltv.user_type_crm,
        final_ga_table.source,
        final_ga_table.medium,
        final_ga_table.first_source,
        final_ga_table.first_medium,
        final_ga_table.platform,
        crm_revenue_ltv.payment_method,
        UPPER(crm_revenue_ltv.platform) AS platform_crm,
        crm_revenue_ltv.gender,
        crm_revenue_ltv.birth_date,
        crm_revenue_ltv.transaction_id,
        crm_revenue_ltv.status,
        crm_revenue_ltv.transactions,
        crm_revenue_ltv.revenue,
        crm_revenue_ltv.margin,
        crm_revenue_ltv.ltv,
        crm_revenue_ltv.all_orders
      FROM
        crm_revenue_ltv
      LEFT JOIN
        final_ga_table
      ON
        crm_revenue_ltv.client_id = final_ga_table.user_pseudo_id
        AND crm_revenue_ltv.date = final_ga_table.date
        AND crm_revenue_ltv.order_date >= final_ga_table.session_start
        AND crm_revenue_ltv.order_date <= final_ga_table.session_end),

--if user_pseudo_id in the previous table is null, takes user_id form the crm    
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
          WHEN user_pseudo_id IS NULL and  client_id IS NOT NULL THEN client_id
          WHEN user_pseudo_id IS NULL and client_id IS NULL THEN user_id
        ELSE
        user_pseudo_id
      END
        AS user_pseudo_id,
        CASE
          WHEN user_type IS NULL THEN user_type_crm
        ELSE
        user_type
      END
        AS user_type,
        source,
        medium,
        first_source,
        first_medium,
        CASE
          WHEN platform IS NULL THEN platform_crm
        ELSE
        platform
      END
        AS platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        transactions,
        revenue,
        margin,
        ltv,
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
        user_type,
        source,
        medium,
        first_source,
        first_medium,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        CASE WHEN status = 'Success' THEN 1
        ELSE 0 END as status,
        isFirst,
        transactions,
        revenue,
        margin,
        ltv,
        all_orders
      FROM
        crm_revenue_ga_is_first
      ),

--makes UNION ALL with final_ga_table to get all user activity
      all_sessions_transactions AS(
      SELECT
        date,
        session_id,
        session_start,
        CAST(NULL AS TIMESTAMP) AS order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        first_source,
        first_medium,
        platform,
        CAST(NULL AS STRING) AS payment_method,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS birth_date,
        CAST (NULL AS STRING) AS transaction_id,
        0 AS status,
        null as isFirst,
        views,
        sign_up,
        addtocart,
        0 AS transactions,
        0 AS revenue,
        0 AS margin,
        0 AS ltv,
        0 as all_orders
      FROM
        final_ga_table
      WHERE
        views IS NOT NULL
      UNION ALL
      SELECT
        date,
        session_id,
        session_start,
        order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        first_source,
        first_medium,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        status,
        isFirst,
        0 AS views,
        0 AS sign_up,
        0 AS addtocart,
        transactions,
        revenue,
        margin,
        ltv,
        all_orders
      FROM
        fix_session_id),

--adds empty parameters
      empty_pam as (
      SELECT
        date,
        session_id,
        session_start,
        CASE WHEN order_date IS NULL THEN first_value(order_date IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id order by session_start)
        ELSE order_date
        END 
        AS order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        first_source,
        first_medium,
        platform,
        CASE WHEN payment_method IS NULL THEN first_value(payment_method IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id order by session_start)
        ELSE payment_method
        END 
        AS payment_method,
        CASE WHEN gender IS NULL THEN first_value(gender) OVER (PARTITION BY user_pseudo_id, session_id order by session_start)
        ELSE gender
        END 
        AS gender,
        CASE WHEN birth_date IS NULL THEN first_value(birth_date IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id order by session_start)
        ELSE birth_date
        END 
        AS birth_date,
        CASE WHEN transaction_id IS NULL THEN first_value(transaction_id IGNORE NULLS) OVER (PARTITION BY user_pseudo_id, session_id order by session_start)
        ELSE transaction_id
        END 
        AS transaction_id,
        status,
        isFirst,
        views,
        sign_up,
        addtocart,
        transactions,
        revenue,
        margin,
        ltv,
        all_orders
        FROM all_sessions_transactions

      ),

-- aggregates all
empty_pam_agg as (

  SELECT 
  date,
        session_id,
        session_start,
        order_date
        order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        first_source,
        first_medium,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id,
        sum(status) as status,
        sum(isFirst) as isFirst,
        sum(views) as views,
        sum(sign_up) as sign_up,
        sum(addtocart) as addtocart,
        sum(transactions) as transactions,
        sum(revenue) as revenue,
        sum(margin) as margin,
        sum(ltv) as ltv,
        sum(all_orders) as all_orders
        FROM empty_pam
        GROUP BY
        date,
        session_id,
        session_start,
        order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        first_source,
        first_medium,
        platform,
        payment_method,
        gender,
        birth_date,
        transaction_id
),

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
  empty_pam_agg.date,
        empty_pam_agg.session_id,
        empty_pam_agg.session_start,
        empty_pam_agg.order_date,
        empty_pam_agg.user_pseudo_id,
        empty_pam_agg.user_type,
        empty_pam_agg.source,
        empty_pam_agg.medium,
        empty_pam_agg.first_source,
        empty_pam_agg.first_medium,
        empty_pam_agg.platform,
        empty_pam_agg.payment_method,
        empty_pam_agg.gender,
        empty_pam_agg.birth_date,
        empty_pam_agg.transaction_id,
        empty_pam_agg.status,
        empty_pam_agg.isFirst,
        empty_pam_agg.views,
        empty_pam_agg.sign_up,
        empty_pam_agg.addtocart,
        empty_pam_agg.transactions,
        empty_pam_agg.revenue,
        empty_pam_agg.margin,
        empty_pam_agg.ltv,
        empty_pam_agg.all_orders,
        products_table.products,
        products_table.delivery
  FROM
    empty_pam_agg
  LEFT JOIN
    products_table
  ON
    empty_pam_agg.transaction_id = products_table.transaction_id ),

--agregates all data
      all_views_transactions_agg AS (
      SELECT
        session_id,
        date,
        session_start,
        order_date,
        user_pseudo_id,
        user_type,
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
        first_source,
        first_medium,
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
        all_orders
      FROM
        crm_revenue_agg),

--adds signup to order rate
signup_to_order as (

  SELECT *, CASE WHEN sum(sign_up) over (partition by user_pseudo_id order by session_start ROWS between unbounded preceding and current row ) > 0 THEN 1
  WHEN sum(transactions) over (partition by user_pseudo_id order by session_start ROWS between unbounded preceding and current row) > 0 THEN 1
  ELSE 0 END as isSignup, 
  CASE WHEN sum(transactions) over (partition by user_pseudo_id order by session_start ROWS between unbounded preceding and current row) > 0 THEN 1
  ELSE 0 END as isOrder
     FROM all_views_transactions_agg
),

--add last session_start time where source is not direct 
      last_non_direct_time AS(
      SELECT
        *,
        MAX(CASE
            WHEN CONCAT(source, '/', medium) != '(direct)/(none)' THEN session_start
        END
          ) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) AS max_non_direct_time
      FROM
        signup_to_order ),

--in case wgen session start time is bigger then last non direct time returns sum of conversions        
      last_direct_revenue AS (
      SELECT
        *,
        SUM(CASE
            WHEN session_start < max_non_direct_time THEN NULL
          ELSE
          sign_up
        END
          ) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS UNBOUNDED PRECEDING) AS max_direct_sign_up,
        SUM(CASE
            WHEN session_start < max_non_direct_time THEN NULL
          ELSE
          transactions
        END
          ) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS UNBOUNDED PRECEDING) AS max_direct_transactions,
        SUM(CASE
            WHEN session_start < max_non_direct_time THEN NULL
          ELSE
          revenue
        END
          ) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS UNBOUNDED PRECEDING) AS max_direct_revenue,
        SUM(CASE
            WHEN session_start < max_non_direct_time THEN NULL
          ELSE
          margin
        END
          ) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS UNBOUNDED PRECEDING) AS max_direct_margin
      FROM
        last_non_direct_time ),

--returns last non direct attribution
      last_non_direct_agg AS (
      SELECT
        date,
        session_id,
        session_start,
        order_date,
        user_pseudo_id,
        user_type,
        source,
        medium,
        first_source,
        first_medium,
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
        all_orders,
        transactions,
        revenue,
        margin,
        CASE
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)' AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN sign_up
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)'
        AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN LAST_VALUE(max_direct_sign_up) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)' AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN sign_up
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)'
        AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN 0
        ELSE
        sign_up
      END
        AS last_non_direct_sign_up,
        CASE
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)' AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN transactions
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)'
        AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN LAST_VALUE(max_direct_transactions) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)' AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN transactions
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)'
        AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN 0
        ELSE
        transactions
      END
        AS last_non_direct_transactions,
        CASE
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)' AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN revenue
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)'
        AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN LAST_VALUE(max_direct_revenue) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)'
        AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN 0
        ELSE
        revenue
      END
        AS last_non_direct_revenue,
        CASE
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)' AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN margin
          WHEN CONCAT(source, '/', medium) != '(direct)/(none)'
        AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN LAST_VALUE(max_direct_margin) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)' AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN margin
          WHEN CONCAT(source, '/', medium) = '(direct)/(none)'
        AND FIRST_VALUE(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN 0
        ELSE
        margin
      END
        AS last_non_direct_margin,
      FROM
        last_direct_revenue),

--aggregates all to simple table
--returns first session time
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
        last_non_direct_sign_up,
        last_non_direct_transactions,
        last_non_direct_revenue,
        last_non_direct_margin
      FROM
        last_non_direct_agg),
      firts_session_time AS (
      SELECT
        *,
        FIRST_VALUE(session_start) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) AS first_session_time
      FROM
        last_non_direct ),

--returns first click attribution
      final_table AS (
      SELECT
        *,
        CASE
          WHEN session_start = first_session_time THEN SUM(sign_up) OVER (PARTITION BY user_pseudo_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE
        0
      END
        AS first_sign_up,
        CASE
          WHEN session_start = first_session_time THEN SUM(transactions) OVER (PARTITION BY user_pseudo_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE
        0
      END
        AS first_transactions,
        CASE
          WHEN session_start = first_session_time THEN SUM(revenue) OVER (PARTITION BY user_pseudo_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE
        0
      END
        AS first_revenue,
        CASE
          WHEN session_start = first_session_time THEN SUM(margin) OVER (PARTITION BY user_pseudo_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE
        0
      END
        AS first_margin,
      FROM
        firts_session_time )
--final query
   SELECT 
    * 
   FROM final_table 
   WHERE views IS NOT NULL