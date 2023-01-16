{{ config(materialized="incremental") }}

with
    -- Собирает базоввые колонки для отчета
    -- Распаковывае параметры для отпределения источника и канала
    -- ! Нужно добавить дату в префиксе таблицы
    base_ga as (
        select
            cast(parse_date('%Y%m%d', event_date) as date) as date,
            event_timestamp,
            event_name,
            user_pseudo_id,
            user_id,
            traffic_source.source as first_source,
            traffic_source.medium as first_medium,
            traffic_source.name as first_campaign,
            platform,
            device.category,
            device.operating_system,
            device.operating_system_version,
            device.web_info.browser as browser,
            ecommerce.transaction_id,
            (
                select value.string_value from unnest(event_params) where key = "source"
            ) as source,
            (
                select value.string_value from unnest(event_params) where key = "medium"
            ) as medium,
            (
                select value.string_value
                from unnest(event_params)
                where key = "campaign"
            ) as campaign,
            (
                select value.string_value
                from unnest(event_params)
                where key = "page_location"
            ) as page_path,
            concat(
                user_pseudo_id,
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = "ga_session_id"
                )
            ) as session_id,
            case
                when event_name = 'page_view'
                then 1
                when event_name = 'screen_view'
                then 1
                else 0
            end as views,
            case when event_name = 'purchase' then 1 else 0 end as transactions
        from `turbo-ukr.analytics_286195171.events_*`
        where
            _table_suffix
            = format_date('%Y%m%d', date_sub(current_date(), interval 2 day))
            and (
                event_name = 'page_view'
                or event_name = 'screen_view'
                or event_name = 'session_start'
                or event_name = 'purchase'
            )
    ),

    -- делает базовую таблицу для опредления источника
    -- проставляет первый канал, источник и кампанию по окну сессии если он есть
    base_ga_sources as (
        select distinct
            (session_id),
            event_name,
            first_source,
            first_medium,
            first_campaign,
            page_path,
            source,
            medium,
            campaign,
            first_value(source ignore nulls) over (
                partition by session_id order by event_timestamp
            ) as first_source_session,
            first_value(medium ignore nulls) over (
                partition by session_id order by event_timestamp
            ) as first_medium_session,
            first_value(campaign ignore nulls) over (
                partition by session_id order by event_timestamp
            ) as first_campaign_session,
            platform
        from base_ga
        where
            event_name = 'page_view'
            or event_name = 'screen_view'
            or event_name = 'session_start'
    ),

    -- прасит UTM и разбивает на разные колонки
    -- приоритеты: приложение - первый источник, UTM - 1, поля параметров -2, первый
    -- заполненный параметр в окне сессии - 3
    -- проставляет ранк по окну сессии по названиям ивентов
    sessions_sources_gross as (
        select distinct
            (session_id),
            event_name,
            case
                when platform = 'ANDROID'
                then first_source
                when platform = 'IOS'
                then first_source
                when page_path like '%utm_source=%'
                then
                    replace(
                        regexp_extract(page_path, r'.*utm_source=([^&]+).*'),
                        '%5C',
                        '\\'
                    )
                when source is not null
                then source
                when source is null
                then first_source_session
                when first_source_session is null
                then '(direct)'
                else '(direct)'
            end as source,
            case
                when platform = 'ANDROID'
                then first_medium
                when platform = 'IOS'
                then first_medium
                when page_path like '%utm_medium=%'
                then
                    replace(
                        regexp_extract(page_path, r'.*[&?]utm_medium=([^&]+).*'),
                        '%5C',
                        '\\'
                    )
                when medium is not null
                then medium
                when medium is null
                then first_medium_session
                when first_medium_session is null
                then '(none)'
                else '(none)'
            end as medium,
            case
                when platform = 'ANDROID'
                then first_campaign
                when platform = 'IOS'
                then first_campaign
                when page_path like '%utm_campaign=%'
                then
                    replace(
                        regexp_extract(page_path, r'.*[&?]utm_campaign=([^&]+).*'),
                        '%5C',
                        '\\'
                    )
                when campaign is not null
                then campaign
                when campaign is null
                then first_campaign_session
                when first_campaign_session is null
                then '(direct)'
                else '(direct)'
            end as campaign,
            regexp_extract(page_path, r'.*[&?]campaign_id=([^&]+).*') as campaign_id,
            replace(
                regexp_extract(page_path, r'.*[&?]utm_content=([^&]+).*'), '%5C', '\\'
            ) as ad_group,
            regexp_extract(page_path, r'.*[&?]adset_id=([^&]+).*') as ad_group_id,
            regexp_extract(page_path, r'.*[&?]utm_ad=([^&]+).*') as ad_id,
            rank() over (partition by session_id order by event_name desc) as row
        from base_ga_sources
        where
            event_name = 'session_start'
            or event_name = 'page_view'
            or event_name = 'screen_view'
    ),

    -- убирает дубли по ранку из предыдущего запроса
    -- проставляет direct/none там где не попало под сценарий
    sessions_sources as (
        select
            session_id,
            case when source is null then '(direct)' else source end as source,
            case when medium is null then '(none)' else medium end as medium,
            case when campaign is null then '(direct)' else campaign end as campaign,
            campaign_id,
            ad_group,
            ad_group_id,
            ad_id
        from sessions_sources_gross
        where row = 1
    ),

    -- cчитает все view
    ga_views as (
        select
            session_id,
            cast(event_timestamp as string) as date_time,
            sum(views) as views
        from base_ga
        group by session_id, date_time
    ),

    -- Считает доход по сессииям из CRM
    -- !!! Есть дата
    crm_revenue as (
        select
            base_ga.session_id,
            base_ga.user_pseudo_id,
            base_deals.user_id,
            extract(date from base_deals.order_date) as date,
            unix_micros(base_deals.order_date) as date_time,
            count(base_deals.transaction_id) as transactions,
            sum(base_deals.value) as revenue
        from {{ ref("base_deals") }} as base_deals
        left join base_ga on base_ga.transaction_id = base_deals.transaction_id

        {% if is_incremental() %}

        where order_date > (select max(date) from {{ this }})

        {% endif %}

        group by
            base_ga.session_id,
            base_ga.user_pseudo_id,
            base_deals.user_id,
            date,
            date_time
    ),

    -- добавлялет сессии к базовой таблице GA, так как GA не покраывает все транзакции
    -- !!! Есть дата
    revenue_all_sessions as (
        select
            date,
            session_id,
            date_time,
            user_pseudo_id,
            user_id,
            sum(transactions) as transactions,
            sum(revenue) as revenue
        from
            (
                select distinct
                    (session_id),
                    date,
                    cast(event_timestamp as string) as date_time,
                    user_pseudo_id,
                    user_id,
                    null as transactions,
                    null as revenue
                from base_ga
                union all
                select
                    session_id,
                    date,
                    cast(date_time as string) as date_time,
                    user_pseudo_id,
                    user_id,
                    transactions,
                    revenue
                from crm_revenue
            )
        group by date, date_time, session_id, user_pseudo_id, user_id
    ),

    -- Собирает итоговый отчет по GA с доходом и кампаниями
    final_report_ga as (
        select distinct
            (revenue_all_sessions.session_id),
            revenue_all_sessions.date,
            revenue_all_sessions.date_time,
            revenue_all_sessions.user_pseudo_id,
            revenue_all_sessions.user_id,
            sessions_sources.source,
            sessions_sources.medium,
            sessions_sources.campaign,
            sessions_sources.campaign_id,
            sessions_sources.ad_group,
            sessions_sources.ad_group_id,
            sessions_sources.ad_id,
            ga_views.views,
            revenue_all_sessions.transactions,
            revenue_all_sessions.revenue
        from revenue_all_sessions
        left join
            ga_views
            on revenue_all_sessions.session_id = ga_views.session_id
            and revenue_all_sessions.date_time = ga_views.date_time
        left join
            sessions_sources
            on revenue_all_sessions.session_id = sessions_sources.session_id
    ),
    -- LEFT JOIN
    -- base_ga
    -- ON
    -- revenue_all_sessions.session_id = base_ga.session_id ),
    -- Аггрегирует итоговый отчет GA без сессиий
    agg_ga_users as (
        select
            date,
            date_time,
            case
                when user_pseudo_id is null then user_id else user_pseudo_id
            end as user_pseudo_id,
            case when source is null then '(not provided)' else source end as source,
            case when medium is null then '(not provided)' else medium end as medium,
            case
                when campaign is null
                then '(not provided)'
                else
                    replace(
                        replace(
                            replace(replace(campaign, '+', ' '), '%2F', '/'), '%28', '('
                        ),
                        '%29',
                        ')'
                    )
            end as campaign,
            ad_group,
            ad_id,
            sum(views) as views,
            sum(transactions) as transactions,
            sum(revenue) as revenue
        from final_report_ga
        group by
            date, date_time, user_pseudo_id, source, medium, campaign, ad_group, ad_id
    ),

    -- Аггрегирует итоговый отчет GA без сессиий, пользователей и времени
    agg_ga as (
        select
            date,
            source,
            medium,
            replace(
                replace(replace(replace(campaign, '+', ' '), '%2F', '/'), '%28', '('),
                '%29',
                ')'
            ) as campaign,
            ad_group,
            ad_id,
            sum(views) as views,
            sum(transactions) as transactions,
            sum(revenue) as revenue
        from final_report_ga
        group by date, source, medium, campaign, ad_group, ad_id
    ),

    -- Аггрегирует данные по рекламе на уровне кампании
    -- !!Есть дата
    campaign_level as (
        select
            date,
            campaign_name as campaign,
            sum(clicks) as clicks,
            sum(impressions) as impressions,
            sum(cost) as cost
        from {{ ref("base_ads") }}
        group by date, campaign
    ),

    -- Выбирает колонки из агг GA только там, где есть кампания и больше никаких
    -- данных по группеили объявлени и добавлет к ним сатут по рекламе
    campaign_level_ads as (
        select
            agg_ga.date,
            agg_ga.source,
            agg_ga.medium,
            agg_ga.campaign,
            agg_ga.ad_group,
            agg_ga.ad_id,
            agg_ga.views,
            agg_ga.transactions,
            agg_ga.revenue,
            campaign_level.clicks,
            campaign_level.impressions,
            campaign_level.cost
        from agg_ga
        left join campaign_level on agg_ga.campaign = campaign_level.campaign

        {% if is_incremental() %}

        where date > (select max(date) from {{ this }})

        {% endif %}

        where
            (agg_ga.source = 'google' or agg_ga.source = 'facebook')
            and agg_ga.medium = 'cpc'
            and agg_ga.ad_group is null
            and agg_ga.ad_id is null
            and agg_ga.campaign is not null
    ),

    -- --Аггрегирует данные по рекламе на уровне группы
    -- !!Есть дата
    ad_group_level as (
        select
            date,
            campaign_name as campaign,
            ad_group_name as ad_group,
            sum(clicks) as clicks,
            sum(impressions) as impressions,
            sum(cost) as cost
        from {{ ref("base_ads") }}

        {% if is_incremental() %}

        where date > (select max(date) from {{ this }})

        {% endif %}

        group by date, campaign, ad_group
    ),

    -- Выбирает колонки из агг GA только там, где есть кампания и группа и больше
    -- никаких данных объявлению и добавлет к ним сатут по рекламе
    ad_group_level_ads as (
        select
            agg_ga.date,
            agg_ga.source,
            agg_ga.medium,
            agg_ga.campaign,
            agg_ga.ad_group,
            agg_ga.ad_id,
            agg_ga.views,
            agg_ga.transactions,
            agg_ga.revenue,
            campaign_level.clicks,
            campaign_level.impressions,
            campaign_level.cost
        from agg_ga
        left join campaign_level on agg_ga.campaign = campaign_level.campaign
        where
            (agg_ga.source = 'google' or agg_ga.source = 'facebook')
            and agg_ga.medium = 'cpc'
            and agg_ga.ad_group is not null
            and agg_ga.ad_id is null
            and agg_ga.campaign is not null
    ),

    -- --Аггрегирует данные по рекламе на уровне объявления
    -- !!Есть дата
    ad_level as (
        select
            date,
            campaign_name as campaign,
            ad_group_name as ad_group,
            ad_id,
            sum(clicks) as clicks,
            sum(impressions) as impressions,
            sum(cost) as cost
        from {{ ref("base_ads") }}

        {% if is_incremental() %}

        where date > (select max(date) from {{ this }})

        {% endif %}

        group by date, campaign, ad_group, ad_id
    ),

    -- Выбирает колонки из агг GA где есть кампания, группа и объявления и добавлет к
    -- ним сатут по рекламе
    ad_ads as (
        select
            agg_ga.date,
            agg_ga.source,
            agg_ga.medium,
            agg_ga.campaign,
            agg_ga.ad_group,
            agg_ga.ad_id,
            agg_ga.views,
            agg_ga.transactions,
            agg_ga.revenue,
            campaign_level.clicks,
            campaign_level.impressions,
            campaign_level.cost
        from agg_ga
        left join campaign_level on agg_ga.campaign = campaign_level.campaign
        where
            (agg_ga.source = 'google' or agg_ga.source = 'facebook')
            and agg_ga.medium = 'cpc'
            and agg_ga.ad_group is not null
            and agg_ga.ad_id is not null
            and agg_ga.campaign is not null
    ),

    -- Объеденяет все три таблицы, что получились выше
    ga_ads as (
        select *
        from campaign_level_ads
        union all
        select *
        from ad_group_level_ads
        union all
        select *
        from ad_ads
    ),

    -- Аггрегирует статус по рекламе на уровне объявления и добавляет колонки по
    -- источникам и каналам
    -- Есть дата!
    agg_ads as (
        select
            date,
            case
                when account_type = 'Google Ads'
                then 'google'
                when account_type = 'Facebook'
                then 'facebook'
            end as source,
            'cpc' as medium,
            campaign_name as campaign,
            campaign_id,
            ad_group_name as ad_group,
            ad_group_id,
            ad_id,
            sum(clicks) as clicks,
            sum(impressions) as impressions,
            sum(cost) as cost
        from {{ ref("base_ads") }} as base_ads

        {% if is_incremental() %}

        where date > (select max(date) from {{ this }})

        {% endif %}

        group by
            date, source, medium, campaign, campaign_id, ad_group, ad_group_id, ad_id
    ),

    -- Выбирает из таблице выше только те строки, которых нет в итоговой таблице по ГА
    ads_diff as (
        select * from agg_ads where campaign not in (select campaign from ga_ads)
    ),

    -- Последний отчет 
    -- Берет таблицу с рекламными данными из ГА, добавляет те строки по рекламной
    -- стате, что не попали в ГА и добавляет все не рекламные остальные каналы
    all_ads as (
        select
            date,
            source,
            medium,
            campaign,
            ad_group,
            ad_id,
            views,
            transactions,
            revenue,
            clicks,
            impressions,
            cost
        from ga_ads
        union all
        select
            date,
            source,
            medium,
            campaign,
            ad_group,
            ad_id,
            null as views,
            null as transactions,
            null as revenue,
            clicks,
            impressions,
            cost
        from ads_diff
        union all
        select
            date,
            case when source is null then '(not provided)' else source end as source,
            case when medium is null then '(not provided)' else medium end as medium,
            case
                when campaign is null then '(not provided)' else campaign
            end as campaign,
            ad_group,
            ad_id,
            views,
            transactions,
            revenue,
            null as clicks,
            null as impressions,
            null as cost
        from agg_ga
        where
            (source != 'google' or source != 'facebook')
            and medium != 'cpc'
            or source is null
    )

select
    date,
    cast(timestamp_micros(cast(date_time as int64)) as datetime) as date_time,
    user_pseudo_id,
    sum(views) as views,
    sum(transactions) as orders,
    sum(revenue) as revenue
from agg_ga_users
group by date, date_time, user_pseudo_id
