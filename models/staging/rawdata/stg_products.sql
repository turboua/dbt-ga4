{{ config(materialized='incremental') }}

select
    transaction_id,
    product.product_name,
    product.product_id,
    product.price,
    product.margin,
    product.product_category,
    product.quantity
from {{ source("raw_db", "raw_deals") }}, unnest(products) as product

-- this filter will only be applied on an incremental run
{% if is_incremental() %}

 where created_at > (select max(created_at) from {{ this }}) 

{% endif %}

group by 1, 2, 3, 4, 5, 6,7
