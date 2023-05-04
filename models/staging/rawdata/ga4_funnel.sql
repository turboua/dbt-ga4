select
    event_date_dt,
    platform,
    source,
    medium,
    count(step1_id) as sign_up,
    count(distinct step2_id) as addtocart,
    count(distinct step3_id) as purchase
from
    (
        select distinct
            event_date_dt,
            platform,
            traffic_source_source as source,
            traffic_source_medium as medium,
            user_pseudo_id as step1_id,
            event_timestamp as step1_timestamp,
            step2_id,
            step2_timestamp,
            step3_id,
            step3_timestamp
        from {{ ref("base_ga4__events") }}

        left join
            (
                select user_pseudo_id as step2_id, step2_timestamp
                from
                    (
                        select
                            user_pseudo_id,
                            event_timestamp as step2_timestamp,
                            row_number() over (
                                partition by user_pseudo_id order by event_timestamp asc
                            ) as row_num
                        from {{ ref("base_ga4__events") }}
                        where event_name = "add_to_cart"
                    )
                where row_num = 1
            )
            on user_pseudo_id = step2_id
            and event_timestamp < step2_timestamp

        left join
            (
                select distinct
                    user_pseudo_id as step3_id, event_timestamp as step3_timestamp,
                from {{ ref("base_ga4__events") }}
                where event_name = "purchase"
            )
            on step3_id = step2_id
            and step2_timestamp < step3_timestamp

        where event_name = "sign_up"
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )
group by 1, 2, 3, 4
order by 1 desc
