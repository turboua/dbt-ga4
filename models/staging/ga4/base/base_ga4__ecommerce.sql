{{ config(materialized="incremental") }}

with
    base_table as (
        select
            event_name,
            event_date_dt,
            event_timestamp,
            user_pseudo_id,
            item.item_name,
            item.item_id,
            item.item_category,
            item.item_brand,
            ecommerce.transaction_id,
            traffic_source_source,
            traffic_source_medium,
            traffic_source_name as campaign,
        from {{ ref("base_ga4__events") }}, unnest(items) as item
    ),

    adds as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'add_to_cart'
        group by 1, 2, 3, 4
    ),

    signups as (
        select distinct event_timestamp, event_name, user_pseudo_id
        from base_table
        where event_name = 'signup'
        group by 1, 2, 3
    ),

    payment_added as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'add_payment_info'
        group by 1, 2, 3, 4
    ),

    shipping_added as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'add_shipping_info'
        group by 1, 2, 3, 4
    ),

    purchase as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'purchase'
        group by 1, 2, 3, 4
    ),

    removed as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'removed_from_cart'
        group by 1, 2, 3, 4
    ),

    wishlist as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'add_to_wishlist'
        group by 1, 2, 3, 4
    ),

    view_item as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'view_item'
        group by 1, 2, 3, 4
    ),

    view_cart as (
        select distinct event_timestamp, event_name, user_pseudo_id
        from base_table
        where event_name = 'view_cart'
        group by 1, 2, 3
    ),

    view_item_list as (
        select distinct event_timestamp, event_name, item_id, user_pseudo_id
        from base_table
        where event_name = 'view_item_list'
        group by 1, 2, 3, 4
    )

select
    b.event_date_dt,
    b.user_pseudo_id,
    b.item_name,
    b.item_id,
    b.item_category,
    b.item_brand,
    b.traffic_source_source as source,
    b.traffic_source_medium as medium,
    campaign,
    count(
        distinct case
            when b.event_name = "session_start"
            then concat(b.user_pseudo_id, b.event_timestamp)
        end
    ) as sessions,
    transaction_id,
    count(s.event_name) as signups,
    count(vl.event_name) as view_item_list,
    count(vi.event_name) as view_item,
    count(pa.event_name) as payment_added,
    count(sa.event_name) as shipping_added,
    count(r.event_name) as removed_from_cart,
    count(w.event_name) as add_to_wishlist,
    count(vc.event_name) as view_cart,
    count(a.event_name) as add_to_cart,
    count(p.event_name) as purchase
from base_table b

left join
    adds a
    on b.event_timestamp = a.event_timestamp
    and b.user_pseudo_id = a.user_pseudo_id
    and b.item_id = a.item_id
    and b.event_name = a.event_name

left join
    signups s
    on b.event_timestamp = s.event_timestamp
    and b.user_pseudo_id = s.user_pseudo_id
    and b.event_name = s.event_name

left join
    payment_added pa
    on b.event_timestamp = pa.event_timestamp
    and b.user_pseudo_id = pa.user_pseudo_id
    and b.item_id = pa.item_id
    and b.event_name = pa.event_name

left join
    shipping_added sa
    on b.event_timestamp = sa.event_timestamp
    and b.user_pseudo_id = sa.user_pseudo_id
    and b.item_id = sa.item_id
    and b.event_name = sa.event_name

left join
    purchase p
    on b.event_timestamp = p.event_timestamp
    and b.user_pseudo_id = p.user_pseudo_id
    and b.item_id = p.item_id
    and b.event_name = p.event_name

left join
    removed r
    on b.event_timestamp = r.event_timestamp
    and b.user_pseudo_id = r.user_pseudo_id
    and b.item_id = r.item_id
    and b.event_name = r.event_name

left join
    wishlist w
    on b.event_timestamp = w.event_timestamp
    and b.user_pseudo_id = w.user_pseudo_id
    and b.item_id = w.item_id
    and b.event_name = w.event_name

left join
    view_item vi
    on b.event_timestamp = vi.event_timestamp
    and b.user_pseudo_id = vi.user_pseudo_id
    and b.item_id = vi.item_id
    and b.event_name = vi.event_name

left join
    view_cart vc
    on b.event_timestamp = vc.event_timestamp
    and b.user_pseudo_id = vc.user_pseudo_id

left join
    view_item_list vl
    on b.event_timestamp = vl.event_timestamp
    and b.user_pseudo_id = vl.user_pseudo_id
    and b.item_id = vl.item_id
    and b.event_name = vl.event_name


{% if is_incremental() %}
where event_date_dt > (select max(event_date_dt) from {{ this }})
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 11
