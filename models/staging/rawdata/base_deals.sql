{{ config(materialized='incremental') }}

select
    created_at as order_date,
    transaction_id,
    user_id,
    value,
    status,
    cid as clien_id,
    sum(product.quantity) as quantity,
    utm_source as source,
    utm_medium as medium,
    utm_content,
    utm_term,
    utm_campaign as campaign,
    platform,
    payment_method,
    delivery.delivery,
    delivery.courier_id,
    delivery.delivery_address,
    delivery.picked_up_at,
    delivery.closed_at
from
    {{ source("raw_db", "raw_deals") }},
    unnest(products) as product,
    unnest(delivery) as delivery

-- this filter will only be applied on an incremental run
{% if is_incremental() %} where created_at > (select max(created_at) from {{ this }}) {% endif %}

group by 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19

union all

select
    created_at as order_date,
    transaction_id,
    user_id,
    value,
    case when transaction_id is not null then 'Refunded' else null end as status,
    '' as client_id,
    sum(product.quantity) as quantity,
    '' as source,
    '' as medium,
    '' as utm_content,
    '' as utm_term,
    '' as campaign,
    '' as platform,
    '' as payment_method,
    '' as delivery,
    null as courier_id,
    '' as delivery_address,
    null as picked_up_at,
    null as closed_at
from {{ source("raw_db", "raw_refunds") }}, unnest(products) as product

-- this filter will only be applied on an incremental run
{% if is_incremental() %} where created_at > (select max(created_at) from {{ this }}) {% endif %}

group by 1, 2, 3, 4, 5, 6, 8, 9, 10
