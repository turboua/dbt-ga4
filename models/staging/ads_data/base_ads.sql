{{ config(materialized='table')}}

select
    date,
    'Google Ads' as account_type,
    account_name,
    campaign_type,
    campaign_name,
    campaign_id,
    ad_group_name,
    ad_group_id,
    ad_id,
    'Default Google ad' as ad_name,
    impressions,
    clicks,
    costmicros as cost
from {{ source("raw_ads", "raw_gads_campaigns") }}

union all

select
    f.date,
    'Facebook' as account_type,
    'Default Facebook account' as account_name,
    'Default Facebook campaign' as campaign_type,
    campaign_name,
    campaign_id,
    adset_name as ad_group_name,
    adset_id as ad_group_id,
    ad_id,
    ad_name,
    impressions,
    clicks,
    case when rate is not null then spend*rate
    when rate is null then spend*LAST_VALUE(rate IGNORE NULLS) OVER(ORDER BY date_sub(f.date, interval 1 day))
    else spend*rate
    end as cost
from {{ source("raw_ads", "raw_facebook") }} f
left join {{ source("raw_ads", "raw_currency") }} c on f.date = c.date