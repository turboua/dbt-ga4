{{ config(materialized='table')}}

WITH
  
---takes only source data from ga table
  base_ga_table AS (
  SELECT
    date,
    source,
    medium,
    replace(replace(replace(replace(
                replace(replace(replace(campaign, '+', ' '), '%2F', '/'), '%28', '('),
                '%29',
                ')'
            ), '%2528', '('), '%255C', '\\'), '%2529', ')') as campaign,
    campaign_id,
    ad_group,
    ad_group_id,
    ad_id,
    SUM(views) AS views,
    SUM(last_non_direct_transactions) AS transactions,
    SUM(last_non_direct_revenue) AS revenue
  FROM
    {{ ref("stg_lastnondirect") }}
  GROUP BY
    date,
    source,
    medium,
    campaign,
    campaign_id,
    ad_group,
    ad_group_id,
    ad_id ),

 ---aggregates data from the ads table on the campaign level   
  campaign_level AS (
  SELECT
    date,
    campaign_name AS campaign,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(cost) AS cost
  FROM
    {{ ref("base_ads") }}
  GROUP BY
    date,
    campaign ),
  
---joins ads data with ga table only when cmapaign is available
  campaign_level_ads AS (
  SELECT
    base_ga_table.date,
    base_ga_table.source,
    base_ga_table.medium,
    base_ga_table.campaign,
    base_ga_table.ad_group,
    base_ga_table.ad_id,
    base_ga_table.views,
    base_ga_table.transactions,
    base_ga_table.revenue,
    campaign_level.clicks,
    campaign_level.impressions,
    campaign_level.cost
  FROM
    base_ga_table
  LEFT JOIN
    campaign_level
  ON
    base_ga_table.campaign = campaign_level.campaign
    AND base_ga_table.date = campaign_level.date
  WHERE
    (base_ga_table.source = 'google'
      OR base_ga_table.source = 'facebook')
    AND base_ga_table.medium = 'cpc'
    AND base_ga_table.ad_group IS NULL
    AND base_ga_table.ad_id IS NULL
    AND base_ga_table.campaign IS NOT NULL),

 ---aggregates data from the ads table on the ad_group level 
  ad_group_level AS (
  SELECT
    date,
    campaign_name AS campaign,
    ad_group_name AS ad_group,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(cost) AS cost
  FROM
    {{ ref("base_ads") }}
  GROUP BY
    date,
    campaign,
    ad_group ),

---joins ads data with ga table only when cmapaign and ad_group are available
  ad_group_level_ads AS (
  SELECT
    base_ga_table.date,
    base_ga_table.source,
    base_ga_table.medium,
    base_ga_table.campaign,
    base_ga_table.ad_group,
    base_ga_table.ad_id,
    base_ga_table.views,
    base_ga_table.transactions,
    base_ga_table.revenue,
    ad_group_level.clicks,
    ad_group_level.impressions,
    ad_group_level.cost
  FROM
    base_ga_table
  LEFT JOIN
    ad_group_level
  ON
  base_ga_table.campaign = ad_group_level.campaign and
    base_ga_table.ad_group = ad_group_level.ad_group
    AND base_ga_table.date = ad_group_level.date
  WHERE
    (base_ga_table.source = 'google'
      OR base_ga_table.source = 'facebook')
    AND base_ga_table.medium = 'cpc'
    AND base_ga_table.ad_group IS NOT NULL
    AND base_ga_table.ad_id IS NULL
     AND base_ga_table.campaign IS NOT NULL),

---aggregates data from the ads table on the ad level 
  ad_level AS (
  SELECT
    date,
    campaign_name AS campaign,
    ad_group_name AS ad_group,
    ad_id,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(cost) AS cost
  FROM
    {{ ref("base_ads") }}
  GROUP BY
    date,
    campaign,
    ad_group,
    ad_id ),

---joins ads data with ga table only when cmapaign, ad_group and ad are available
  ad_ads AS (
  SELECT
    base_ga_table.date,
    base_ga_table.source,
    base_ga_table.medium,
    base_ga_table.campaign,
    base_ga_table.ad_group,
    base_ga_table.ad_id,
    base_ga_table.views,
    base_ga_table.transactions,
    base_ga_table.revenue,
    ad_level.clicks,
    ad_level.impressions,
    ad_level.cost
  FROM
    base_ga_table
  LEFT JOIN
    ad_level
  ON
  base_ga_table.campaign = ad_level.campaign and
  base_ga_table.ad_group = ad_level.ad_group and
    base_ga_table.ad_id = ad_level.ad_id
    AND base_ga_table.date = ad_level.date
  WHERE
    (base_ga_table.source = 'google'
      OR base_ga_table.source = 'facebook')
    AND base_ga_table.medium = 'cpc'
    AND base_ga_table.ad_group IS NOT NULL
    AND base_ga_table.ad_id IS NOT NULL
     AND base_ga_table.campaign IS NOT NULL),

---unions all three tables
  ga_ads AS (
  SELECT
    *
  FROM
    campaign_level_ads
  UNION ALL
  SELECT
    *
  FROM
    ad_group_level_ads
  UNION ALL
  SELECT
    *
  FROM
    ad_ads ),
  
--takes all ads data from the ads table
  agg_ads AS (
  SELECT
    date,
    CASE
      WHEN account_type = 'Google Ads' THEN 'google'
      WHEN account_type = 'Facebook' THEN 'facebook'
  END
    AS source,
    'cpc' AS medium,
    campaign_name AS campaign,
    campaign_id,
    ad_group_name AS ad_group,
    ad_group_id,
    ad_id,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(cost) AS cost
  FROM
    {{ ref("base_ads") }}
  GROUP BY
    date,
    source,
    medium,
    campaign,
    campaign_id,
    ad_group,
    ad_group_id,
    ad_id ),

---takes ads data from the previous table that where not in the ga_ads table
  ads_diff AS (
  SELECT
    *
  FROM
    agg_ads
  WHERE
    campaign NOT IN (
    SELECT
      campaign
    FROM
      ga_ads) ),
  
---unions ga_ads, ad_diff and base_ga data
  all_ads AS (
  SELECT
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id,
    views,
    transactions,
    revenue,
    clicks,
    impressions,
    cost
  FROM
    ga_ads
  UNION ALL
  SELECT
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id,
    NULL AS views,
    NULL AS transactions,
    NULL AS revenue,
    clicks,
    impressions,
    cost
  FROM
    ads_diff
  UNION ALL
  SELECT
    date,
    CASE
      WHEN source IS NULL THEN '(not provided)'
    ELSE
    source
  END
    AS source,
    CASE
      WHEN medium IS NULL THEN '(not provided)'
    ELSE
    medium
  END
    AS medium,
    CASE
      WHEN campaign IS NULL THEN '(not provided)'
    ELSE
    campaign
  END
    AS campaign,
    ad_group,
    ad_id,
    views,
    transactions,
    revenue,
    NULL AS clicks,
    NULL AS impressions,
    NULL AS cost
  FROM
    base_ga_table
  WHERE
    (source != 'google'
      OR source != 'facebook')
    AND medium != 'cpc'
    OR source IS NULL )


SELECT
  date,
  source,
  medium,
  campaign,
  ad_group,
  ad_id,
  views,
  transactions,
  revenue,
  clicks,
  impressions,
  cost
FROM
  all_ads 