{{ config(materialized="table") }}

with
    -- таблица первых посещений с датами и client_id
    first_visit as (
        select date(timestamp_micros(event_timestamp)) as cohort_date, user_pseudo_id
        from {{ ref("base_ga4__events") }}
        where event_name = 'first_visit'
    ),

    -- таблица с общим количеством пользователей, которые посетили сайт 
    -- первый раз по месяцам
    count_first_visit as (
        select extract(month from cohort_date) as month, count(user_pseudo_id) as users
        from first_visit
        group by month
    ),

    -- таблица с разницой посещений пользователей по месяцам
    date_difference as (
        select
            date_diff(
                date(timestamp_micros(ga.event_timestamp)),
                first_visit.cohort_date,
                month
            ) as month_number,
            ga.user_pseudo_id
        from {{ ref("base_ga4__events") }} ga
        left join
            first_visit on ga.user_pseudo_id = first_visit.user_pseudo_id,
            unnest(ga.event_params) as params
        where ga.event_name = 'session_start'
        group by 2, 1
    ),

    -- таблица c годом, номером месяца, месяцем, номером месяца возврата
    retention as (
        select
            extract(year from first_visit.cohort_date) as cohort_year,
            extract(month from first_visit.cohort_date) as cohort_month_num,
            format_date("%B", first_visit.cohort_date) as cohort_month,
            case
                when date_difference.month_number = 0
                then 'Month_0'
                when date_difference.month_number = 1
                then 'Month_1'
                when date_difference.month_number = 2
                then 'Month_2'
                when date_difference.month_number = 3
                then 'Month_3'
                when date_difference.month_number = 4
                then 'Month_4'
                when date_difference.month_number = 5
                then 'Month_5'
                when date_difference.month_number = 6
                then 'Month_6'
                when date_difference.month_number = 7
                then 'Month_7'
                when date_difference.month_number = 8
                then 'Month_8'
                when date_difference.month_number = 9
                then 'Month_9'
                when date_difference.month_number = 10
                then 'Month_10'
                when date_difference.month_number = 11
                then 'Month_11'
                when date_difference.month_number = 12
                then 'Month_12'
                else 'Month_er'
            end as month_number,
            count(first_visit.user_pseudo_id) as num_users,
            first_visit.user_pseudo_id
        from date_difference
        left join
            first_visit on date_difference.user_pseudo_id = first_visit.user_pseudo_id
        group by 1, 2, 3, 4, 6
    ),

    -- итоговая таблица куда еще добавляется количество всех пользователей в 
    -- в месяц 0 для дальнейшего расчета процента возврата.
    final_table as (
        select
            *,
            first_value(num_users) over (
                partition by cohort_month order by month_number
            ) as month_0
        from retention
    )
select
    user_pseudo_id,
    cohort_year,
    cohort_month as month,
    month_number,
    num_users,
    month_0
from final_table
where month_number != 'Month_er'
order by cohort_year, cohort_month desc, month_number
