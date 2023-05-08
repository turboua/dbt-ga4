WITH
  last_non_direct_time AS(
  SELECT
    *,
    MAX(CASE
        WHEN CONCAT(source, '/', medium) != '(direct)/(none)' THEN session_start
    END
      ) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) AS max_non_direct_time
  FROM
    {{ ref("ads_sessions") }} ),
  last_direct_revenue AS (
  SELECT
    *,
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
      ) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS UNBOUNDED PRECEDING) AS max_direct_revenue
  FROM
    last_non_direct_time ),
  last_non_direct_agg AS (
  SELECT
    date,
    session_id,
    session_start,
    user_pseudo_id,
    source,
    medium,
    campaign,
    ad_group,
    ad_id,
    platform,
    views,
    transactions,
    revenue,
    CASE
      WHEN CONCAT(source, '/', medium) != '(direct)/(none)' AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN transactions
      WHEN CONCAT(source, '/', medium) != '(direct)/(none)'
    AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN LAST_VALUE(max_direct_transactions) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
     WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and first_value(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN transactions
       WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and first_value(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN 0
    ELSE
    transactions
  END
    AS last_non_direct_transactions,
    CASE
      WHEN CONCAT(source, '/', medium) != '(direct)/(none)' AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN revenue
      WHEN CONCAT(source, '/', medium) != '(direct)/(none)'
    AND LEAD(CONCAT(source, '/', medium)) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN LAST_VALUE(max_direct_revenue) OVER (PARTITION BY user_pseudo_id, max_non_direct_time ORDER BY session_start ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
      --WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and first_value(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) = '(direct)/(none)' THEN revenue
       WHEN CONCAT(source, '/', medium) = '(direct)/(none)' and first_value(NULLIF(CONCAT(source, '/', medium), '(direct)/(none)') IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_start) != '(direct)/(none)' THEN 0
    ELSE
    revenue
  END
    AS last_non_direct_revenue
  FROM
    last_direct_revenue),
  last_non_direct AS (
  SELECT
    date,
    session_id,
    session_start,
    user_pseudo_id,
    source,
    medium,
    campaign,
    last_non_direct_transactions,
    last_non_direct_revenue,
    ad_group,
    ad_id,
    platform,
    views,
    transactions,
    revenue
  FROM
    last_non_direct_agg),
  final_table AS (
  SELECT
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id,
    platform,
    SUM(views) AS views,
    SUM(transactions) AS transactions,
    SUM(revenue) AS revenue,
    SUM(last_non_direct_transactions) AS last_non_direct_transactions,
    SUM(last_non_direct_revenue) AS last_non_direct_revenue,
  FROM
    last_non_direct
  GROUP BY
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id,
    platform )
SELECT
  *
FROM
  final_table