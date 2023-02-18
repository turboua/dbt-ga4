select
    user_id,
    sum(item_revenue) / count(distinct transaction_id) as aov,
    count(distinct transaction_id)
    / count(distinct format_date('%Y-%m', order_date)) as purchase_frequency,
    round(
        date_diff(max(cast(order_date as date)), min(cast(order_date as date)), month),
        3
    ) as customer_lifetime,
    (
        sum(item_revenue)
        / (count(distinct transaction_id))
        * case
            when
                count(distinct transaction_id) > 0
                and count(distinct format_date('%Y-%m', order_date)) > 0
                and date_diff(max(order_date), min(order_date), day) > 0
            then
                (
                    count(distinct transaction_id)
                    / count(distinct format_date('%Y-%m', order_date))
                ) * round(
                    (
                        (
                            date_diff(
                                max(cast(order_date as date)),
                                min(cast(order_date as date)),
                                month
                            )
                        )
                        / count(distinct user_id)
                    ),
                    0
                )
            else 1
        end
    ) as ltv
from {{ ref("ga4_raw__sources") }}
group by 1
