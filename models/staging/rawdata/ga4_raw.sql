with rawdb as(
    select * from {{ ref('base_products') }}
),

ga4 as(
    select * from {{ ref('base_ga4__ecommerce') }}
)

select order_date, product_name, product_category, source, medium, campaign, platform, payment_method, status,
users, sessions, signups, orders, item_revenue, quantity, margin 
from {{ ref('base_products') }} b left join  {{ ref('base_ga4__ecommerce') }} g
on b.clien_id=g.user_pseudo_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16