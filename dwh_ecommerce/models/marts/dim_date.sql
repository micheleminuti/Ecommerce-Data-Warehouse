{{ config(
    materialized='table',
    schema='marts'
) }}

with date_spine as (
    select
        range as datetime_hour
    from range(
        TIMESTAMP '2015-01-01 00:00:00',
        TIMESTAMP '2025-01-01 00:00:00',
        INTERVAL 1 HOUR
    )
)

select
    row_number() over (order by datetime_hour) as sk_date,
    datetime_hour as date_day,
    year(datetime_hour) as year,
    month(datetime_hour) as month,
    hour(datetime_hour) as hour,
    dayname(datetime_hour) as day_name,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_spine
