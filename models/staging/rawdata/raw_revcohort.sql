{{ config(materialized="table") }}

select
    extract(year from b.date) as cohort_year,
    format_date('%m', b.date) as month_number,
    format_date('%W', b.date) as week_number,
    
     SUM(case
            when date_diff(o.date, b.date, isoweek) = 0 then o.value
        end)
     as week_0,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 1 then o.value
        end
    ) as week_1,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 2 then o.value
        end
    ) as week_2,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 3 then o.value
        end
    ) as week_3,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 4 then o.value
        end
    ) as week_4,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 5 then o.value
        end
    ) as week_5,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 6 then o.value
        end
    ) as week_6,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 7 then o.value
        end
    ) as week_7,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 8 then o.value
        end
    ) as week_8,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 9 then o.value
        end
    ) as week_9,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 10 then o.value
        end
    ) as week_10,
    SUM(
        case
            when date_diff(o.date, b.date, isoweek) = 11 then o.value
        end
    ) as week_11,
    COUNT(
        case
            when date_diff(o.date, b.date, isoweek) = 12 then o.value
        end
    ) as week_12

from {{ source("raw_db", "raw_deals") }} o
join
    (
        select distinct user_id, transaction_id, value, min(date) as date
        from {{ source("raw_db", "raw_deals") }}
        group by user_id, 2,3
    ) b
    on o.user_id = b.user_id
    and o.value=b.value

group by 1, 2,3
order by 1, 2,3