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
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

union all

select
    date,
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
    spend as cost
from {{ source("raw_ads", "raw_facebook") }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
