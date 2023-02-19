{{ config(materialized="incremental") }}

with
    tr as (
        select
            client_id,
            user_id,
            platform,
            order_date,
            transaction_id,
            status,
            value,
            margin,
            quantity
        from {{ ref("base_deals") }}
    ),

    ranked as (
        select
            client_id,
            user_id,
            platform,
            order_date,
            transaction_id,
            value,
            margin,
            status,
            quantity,
            rank() over (partition by client_id order by order_date) as rank
        from tr
    ),

    final as (
        select
            client_id,
            user_id,
            platform,
            order_date,
            transaction_id,
            value,
            margin,
            status,
            quantity,
            rank = 1 as isfirstsale
        from ranked
    )

select *
from final

{% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}