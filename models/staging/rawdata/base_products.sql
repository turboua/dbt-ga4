{{ config(materialized="incremental") }}

select
    order_date,
    d.signup_date,
    p.transaction_id,
    client_id,
    d.birth_date,
    d.gender,
    product_name,
    product_id,
    product_category,
    d.reg_platform,
    platform,
    payment_method,
    status,
    count(distinct d.transaction_id) as orders,
    price,
    p.quantity,
    sum(margin) as margin
from {{ ref("stg_products") }} p
left join {{ ref("base_deals") }} d on p.transaction_id = d.transaction_id

{% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16
