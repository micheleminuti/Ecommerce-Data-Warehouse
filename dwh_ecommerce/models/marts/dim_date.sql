{{ config(
    materialized='table',
    schema='marts'
) }}

with date_spine as (
    select 
        cast(range as DATE) as date_day
    from range(DATE '1950-01-01', DATE '2025-01-01', INTERVAL 1 DAY)
)

select
    cast(strftime(date_day, '%Y%m%d') as int) as date_sk,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    dayname(date_day) as day_name,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_spine
