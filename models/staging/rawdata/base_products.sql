SELECT order_date, product_name, product_category, platform, payment_method, status, count(distinct d.transaction_id) as orders, sum(price * p.quantity) as item_revenue, p.quantity, sum(margin) as margin 
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('base_deals') }} d on p.transaction_id=d.transaction_id
group by 1,2,3,4,5,6,9
order by count(d.transaction_id) desc