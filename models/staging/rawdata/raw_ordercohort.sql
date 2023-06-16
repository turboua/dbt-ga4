{{ config(materialized="table") }}

select
    extract(year from b.date) as cohort_year,
    format_date('%B', b.date) as month,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 0 then o.user_id
        end
    ) as month_0,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 1 then o.user_id
        end
    ) as month_1,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 2 then o.user_id
        end
    ) as month_2,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 3 then o.user_id
        end
    ) as month_3,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 4 then o.user_id
        end
    ) as month_4,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 5 then o.user_id
        end
    ) as month_5,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 6 then o.user_id
        end
    ) as month_6,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 7 then o.user_id
        end
    ) as month_7,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 8 then o.user_id
        end
    ) as month_8,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 9 then o.user_id
        end
    ) as month_9,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 10 then o.user_id
        end
    ) as month_10,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 11 then o.user_id
        end
    ) as month_11,
    count(
        distinct case
            when date_diff(o.date, b.date, month) = 12 then o.user_id
        end
    ) as month_12

from {{ source("raw_db", "raw_deals") }} o
join
    (
        select user_id, transaction_id, min(date) as date
        from {{ source("raw_db", "raw_deals") }}
        group by user_id, 2
    ) b
    on o.user_id = b.user_id
group by 1, 2
order by 1, 2
