select
    order_date,
    p.transaction_id,
    clien_id,
    product_name,
    product_category,
    platform,
    payment_method,
    status,
    count(distinct d.transaction_id) as orders,
    sum(price * p.quantity) as item_revenue,
    p.quantity,
    sum(margin) as margin
from {{ ref("stg_products") }} p
left join {{ ref("base_deals") }} d on p.transaction_id = d.transaction_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 11
