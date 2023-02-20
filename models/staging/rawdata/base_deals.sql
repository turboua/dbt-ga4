{{ config(materialized="incremental") }}

select
    d.created_at as order_date,
    u.created_at as signup_date,
    transaction_id,
    d.user_id,
    sex as gender,
    birth_date,
    value,
    round(sum(product.margin), 2) as margin,
    status,
    cid as client_id,
    sum(product.quantity) as quantity,
    utm_source as source,
    utm_medium as medium,
    utm_content,
    utm_term,
    utm_campaign as campaign,
    platform,
    reg_platform,
    payment_method,
    delivery.delivery,
    delivery.courier_id,
    delivery.delivery_address,
    delivery.picked_up_at,
    delivery.closed_at
from
    {{ source("raw_db", "raw_deals") }} d,
    unnest(products) as product,
    unnest(delivery) as delivery
left join {{ source("raw_db", "raw_users") }} u on d.user_id = u.user_id

-- this filter will only be applied on an incremental run
{% if is_incremental() %}
where d.created_at > (select max(order_date) from {{ this }})
{% endif %}


group by 1, 2, 3, 4, 5, 6, 7, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24

union all

select
    r.created_at as order_date,
    timestamp('1900-01-01 00:00:00 UTC') as signup_date,
    transaction_id,
    user_id,
    '' as gender,
    '' as birth_date,
    value,
    0 as margin,
    case when transaction_id is not null then 'Refunded' else null end as status,
    '' as client_id,
    sum(product.quantity) as quantity,
    '' as source,
    '' as medium,
    '' as utm_content,
    '' as utm_term,
    '' as campaign,
    '' as platform,
    '' as reg_platform,
    '' as payment_method,
    '' as delivery,
    null as courier_id,
    '' as delivery_address,
    null as picked_up_at,
    null as closed_at
from {{ source("raw_db", "raw_refunds") }} r, unnest(products) as product

-- this filter will only be applied on an incremental run
{% if is_incremental() %}
where r.created_at > (select max(order_date) from {{ this }})
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7, 8, 9
