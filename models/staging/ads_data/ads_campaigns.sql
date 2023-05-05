{{ config(materialized='table')}}

WITH
  
---takes only source data from ga table
  base_ga_table AS (
  SELECT
    date,
    source,
    medium,
    replace(replace(replace(replace(replace(replace(
                replace(replace(replace(campaign, '+', ' '), '%2F', '/'), '%28', '('),
                '%29',
                ')'
            ), '%2528', '('), '%255C', '\\'), '%2529', ')'), '%7B', '{'), '%7D', '}') as campaign,
    replace(replace(replace(replace(replace(replace(
                replace(replace(replace(ad_group, '+', ' '), '%2F', '/'), '%28', '('),
                '%29',
                ')'
            ), '%2528', '('), '%255C', '\\'), '%2529', ')'), '%7B', '{'), '%7D', '}') as ad_group,
    ad_id,
    SUM(views) AS views,
    SUM(last_non_direct_transactions) AS transactions,
    SUM(last_non_direct_revenue) AS revenue,
    null as impressions,
    null as clicks,
    null as cost,
    
  FROM
    {{ ref("stg_lastnondirect") }}
  GROUP BY
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id ),

 ---aggregates data from the ads table on the campaign level   
  campaign_level AS (
  SELECT
    date,
     CASE
      WHEN account_type = 'Google Ads' THEN 'google'
      WHEN account_type = 'Facebook' THEN 'facebook'
  END
    AS source,
    'cpc' AS medium,
    campaign_name AS campaign,
    cast(null as string) as ad_group,
    cast(null as string) as ad_id,
    null as views,
    null as transactions,
    null as revenue,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost
  FROM
    {{ ref("base_ads") }}
  WHERE campaign_type = 'PERFORMANCE_MAX' or campaign_name like '%IOS%' or campaign_name like '%Android%'
  GROUP BY
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id ),
  
--joins ads data with ga table only when cmapaign is available
  campaign_level_ads AS (
  SELECT
    *
  FROM
    base_ga_table
  UNION ALL
  SELECT
    *
    FROM
    campaign_level),


---aggregates data from the ads table on the ad level 
  ad_level AS (
  SELECT
    date,
    CASE
      WHEN account_type = 'Google Ads' THEN 'google'
      WHEN account_type = 'Facebook' THEN 'facebook'
  END
    AS source,
    'cpc' AS medium,
    campaign_name AS campaign,
    ad_group_name AS ad_group,
    ad_id,
    null as views,
    null as transactions,
    null as revenue,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost
  FROM
    {{ ref("base_ads") }}
  WHERE campaign_type != 'PERFORMANCE_MAX'and campaign_name not like '%IOS%' and campaign_name not like '%Android%'
  GROUP BY
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id ),

---joins ads data with ga table only when cmapaign, ad_group and ad are available
  ad_ads AS (
  SELECT
    *
  FROM
    campaign_level_ads
  UNION ALL
  SELECT * FROM
    ad_level),

---unions all three tables
  ga_ads AS (
  SELECT
    date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id,
    SUM(views)as views,
    SUM(transactions) as transactions,
    SUM(revenue) as revenue,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
    SUM(cost) AS cost
  FROM
    ad_ads
  GROUP BY 
  date,
    source,
    medium,
    campaign,
    ad_group,
    ad_id)


SELECT
  *
FROM
  ga_ads 