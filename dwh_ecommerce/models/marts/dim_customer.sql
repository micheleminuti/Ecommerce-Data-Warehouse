{{ config(
    materialized='table',
    schema='marts'
) }}

with raw_customers as (
    select distinct
        customer_id,
        first_name,
        last_name,
        username,
        email,
        gender,
        birth_date,
        device_type,
        device_version,
        home_location,
        home_country,
        first_join_date
    from {{ ref('customer_ods') }}  -- ✅ Nome corretto
),

enriched as (
    select 
        c.*,
        l.sk_location,
        jd.date_sk as sk_join_date,
        bd.date_sk as sk_birth_date,
        dv.sk_device
    from raw_customers c

    left join {{ ref('dim_location') }} l 
        on l.country_name = c.home_country 
        and l.region_name = c.home_location 

    left join {{ ref('dim_date') }} jd on jd.date_day = c.first_join_date

    left join {{ ref('dim_date') }} bd on bd.date_day = c.birth_date

    left join {{ ref('dim_device') }} dv 
        on dv.device_type = c.device_type 
        and dv.device_version = c.device_version  
)



select 
    row_number() over (order by customer_id) as sk_customer,
    customer_id,
    first_name,
    last_name,
    username,
    email,
    gender,
    sk_location,
    sk_birth_date,
    sk_join_date,
    sk_device
from enriched
