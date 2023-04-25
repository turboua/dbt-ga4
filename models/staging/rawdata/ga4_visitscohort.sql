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
        select extract(week from cohort_date) as week, count(user_pseudo_id) as users
        from first_visit
        group by week
    ),

    -- таблица с разницой посещений пользователей по месяцам
    date_difference as (
        select
            date_diff(
                date(timestamp_micros(ga.event_timestamp)),
                first_visit.cohort_date,
                week
            ) as week_number,
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
            extract(week from first_visit.cohort_date) as cohort_week_num,
            format_date("%W", first_visit.cohort_date) as cohort_week,
            case
                when date_difference.week_number = 0
                then 'Week_0'
                when date_difference.week_number = 1
                then 'Week_1'
                when date_difference.week_number = 2
                then 'Week_2'
                when date_difference.week_number = 3
                then 'Week_3'
                when date_difference.week_number = 4
                then 'Week_4'
                when date_difference.week_number = 5
                then 'Week_5'
                when date_difference.week_number = 6
                then 'Week_6'
                when date_difference.week_number = 7
                then 'Week_7'
                when date_difference.week_number = 8
                then 'Week_8'
                when date_difference.week_number = 9
                then 'Week_9'
                when date_difference.week_number = 10
                then 'Week_10'
                when date_difference.week_number = 11
                then 'Week_11'
                when date_difference.week_number = 12
                then 'Week_12'
                else 'Week_er'
            end as week_number,
            count(first_visit.user_pseudo_id) as num_users
        from date_difference
        left join
            first_visit on date_difference.user_pseudo_id = first_visit.user_pseudo_id
        group by 1, 2, 3, 4
    ),

    -- итоговая таблица куда еще добавляется количество всех пользователей в 
    -- в месяц 0 для дальнейшего расчета процента возврата.
    final_table as (
        select
            *,
            first_value(num_users) over (
                partition by cohort_week order by week_number
            ) as week_0
        from retention
    )
select
    cohort_year,
    cohort_week as week,
    week_number,
    num_users,
    week_0
from final_table
where week_number != 'Week_er'
order by cohort_year, cohort_week desc, week_number