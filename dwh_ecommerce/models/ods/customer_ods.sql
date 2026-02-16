{{ config(
    materialized='table',
    schema='ods'
) }}

with source as (
    select * from {{ ref('customers') }}

),

cleaned as (
    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(username) as username,
        trim(lower(email)) as email,
        upper(gender) as gender,  -- M/F
        birthdate::date as birth_date,  
        device_type,
        device_version,
        trim(upper(home_location)) as home_location,
        trim(upper(home_country)) as home_country,
        first_join_date::date as first_join_date
    from source
    where customer_id is not null
      and length(trim(email)) > 0
      and gender in ('M', 'F')
)

select * from cleaned
