{{ config(materialized="table") }}

select
    orders_per_customer.order_count,
    count(distinct customer_orders.user_id) as customer_count,
    sum(orders_per_customer.value) / sum(orders_per_customer.order_count) as aov
from
    (
        select
            user_id, count(distinct transaction_id) as order_count, sum(value) as value
        from {{ ref("base_deals") }}
        group by user_id
    ) as orders_per_customer
join
    (
        select user_id, count(distinct transaction_id) as order_count,
        from {{ ref("base_deals") }}
        group by user_id
    ) as customer_orders
    on orders_per_customer.order_count = customer_orders.order_count
group by orders_per_customer.order_count
order by orders_per_customer.order_count
