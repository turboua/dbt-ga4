with
    base_table as (
        select
            event_name,
            event_date_dt,
            event_timestamp,
            user_pseudo_id,
            traffic_source_source,
            traffic_source_medium,
            traffic_source_name as campaign
        from {{ ref('base_ga4__events') }}
    ),

    items as (
        select
        event_name,
            user_pseudo_id,
            event_timestamp,
            item.item_name,
            item.item_category,
            item.item_brand
        from {{ ref('base_ga4__events') }}, unnest(items) as item
        where item_name is not null
        group by 1, 2, 3, 4, 5, 6
    ),

    signups as (
        select event_timestamp, user_pseudo_id, count(event_timestamp) as signups
        from {{ ref('base_ga4__events') }}
        where event_name = 'sign_up'
        group by 1, 2
    ),

    adds as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'add_to_cart'
        group by 1, 2,3,4
    ),

    payment_added as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'add_payment_info'
        group by 1, 2,3,4
    ),

    shipping_added as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'add_shipping_info'
        group by 1, 2,3,4
    ),

    purchase as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name, items[SAFE_OFFSET(0)].item_category, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'purchase'
        group by 1, 2,3,4,5
    ),

    removed as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name,items[SAFE_OFFSET(0)].item_category, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'remove_from_cart'
        group by 1, 2,3,4,5
    ),

    wishlist as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name,items[SAFE_OFFSET(0)].item_category,  user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'add_to_wishlist'
        group by 1, 2,3,4,5
    ),

    view_item as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name, items[SAFE_OFFSET(0)].item_category, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'view_item'
        group by 1, 2,3,4,5
    ),

    view_cart as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'view_cart'
        group by 1, 2,3,4
    ),

    view_item_list as (
        select event_name, event_timestamp, items[SAFE_OFFSET(0)].item_name,items[SAFE_OFFSET(0)].item_category, user_pseudo_id
        from {{ ref('base_ga4__events') }}
        where event_name = 'view_item_list'
        group by 1, 2,3,4,5
    )

select
    b.event_date_dt,
    b.user_pseudo_id,
    it.item_name,
    it.item_category,
    item_brand,
    b.traffic_source_source as source,
    b.traffic_source_medium as medium,
    campaign,
    count(distinct b.user_pseudo_id) as users,
    count(
        distinct case
            when b.event_name = "session_start"
            then concat(b.user_pseudo_id, b.event_timestamp)
        end
    ) as sessions,
    count(distinct s.event_timestamp) as signups,
    count(distinct w.event_timestamp) as add_to_wishlist,
    count(distinct re.event_timestamp) as remove_from_cart,
    count(distinct vl.event_timestamp) as view_item_list,
    count(distinct vi.event_timestamp) as view_item,
    count(distinct a.event_timestamp) as add_to_cart,
    count(distinct vc.event_timestamp) as view_cart,
    count(distinct pa.event_timestamp) as add_payment_info,
    count(distinct sa.event_timestamp) as add_shipping_info,
    count(distinct p.event_timestamp) as purchase
from base_table b

left join
    items it
    on b.event_timestamp = it.event_timestamp
    and b.user_pseudo_id = it.user_pseudo_id
left join
    signups s
    on b.event_timestamp = s.event_timestamp
    and b.user_pseudo_id = s.user_pseudo_id
left join
    adds a
    on b.event_timestamp = a.event_timestamp
    and b.user_pseudo_id = a.user_pseudo_id
    and a.item_name = it.item_name
left join
    payment_added pa
    on b.event_timestamp = pa.event_timestamp
    and b.user_pseudo_id = pa.user_pseudo_id
    and pa.item_name = it.item_name
left join
    shipping_added sa
    on b.event_timestamp = sa.event_timestamp
    and b.user_pseudo_id = sa.user_pseudo_id
    and sa.item_name = it.item_name
left join
    purchase p
    on b.event_timestamp = p.event_timestamp
    and b.user_pseudo_id = p.user_pseudo_id
    and p.item_name = it.item_name
    and p.item_category = it.item_category
left join
    removed re
    on b.event_timestamp = re.event_timestamp
    and b.user_pseudo_id = re.user_pseudo_id
    and re.item_name = it.item_name
    and re.item_category = it.item_category
left join
    wishlist w
    on b.event_timestamp = w.event_timestamp
    and b.user_pseudo_id = w.user_pseudo_id
    and w.item_name = it.item_name
    and w.item_category = it.item_category
left join
    view_cart vc
    on b.event_timestamp = vc.event_timestamp
    and b.user_pseudo_id = vc.user_pseudo_id
left join
    view_item vi
    on b.event_timestamp = vi.event_timestamp
    and b.user_pseudo_id = vi.user_pseudo_id
    and vi.item_name = it.item_name
    and vi.item_category = it.item_category
left join
    view_item_list vl
    on b.event_timestamp = vl.event_timestamp
    and b.user_pseudo_id = vl.user_pseudo_id
    and vl.item_name = it.item_name
    and vl.item_category = it.item_category
group by 1, 2, 3, 4, 5, 6, 7, 8