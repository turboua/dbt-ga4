select
    transaction_id,
    product.product_name,
    product.product_id,
    product.price,
    product.margin,
    product.product_category
from {{ source("raw_db", "raw_deals") }}, unnest(products) as product
group by 1, 2, 3, 4, 5, 6
