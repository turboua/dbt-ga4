with rawdb as(
    select * from {{ ref('base_products') }}
),

ga4 as(
    select user_pseudo_id, source, medium, campaign from {{ ref('base_ga4__ecommerce') }}
)

select order_date, transaction_id,  clien_id,  product_name, product_category, source, medium, campaign, platform, payment_method, status, item_revenue, quantity, margin 
from {{ ref('base_products') }} b left join  {{ ref('base_ga4__ecommerce') }} g
on b.clien_id=g.user_pseudo_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14