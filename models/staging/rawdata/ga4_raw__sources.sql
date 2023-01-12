{{ config(materialized="incremental") }}

with
    rawdb as (select * from {{ ref("base_products") }}),

    ga4 as (
        select user_pseudo_id, source, medium, campaign
        from {{ ref("base_ga4__ecommerce") }}
    )

select
    order_date,
    signup_date,
    birth_date,
    b.transaction_id,
    client_id,
    gender,
    product_name,
    product_id,
    product_category,
    source,
    medium,
    campaign,
    reg_platform,
    platform,
    payment_method,
    status,
    price as item_revenue,
    quantity,
    margin
from {{ ref("base_products") }} b
left join {{ ref("base_ga4__ecommerce") }} g on b.client_id = g.user_pseudo_id

{% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
