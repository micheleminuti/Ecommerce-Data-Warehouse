/*
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
        bd.date_sk as sk_birth_date
    from raw_customers c

    left join {{ ref('dim_location') }} l 
        on l.country_name = c.home_country 
        and l.region_name = c.home_location 

    left join {{ ref('dim_date') }} jd on jd.date_day = c.first_join_date

    left join {{ ref('dim_date') }} bd on bd.date_day = c.birth_date

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
    sk_join_date
from enriched

*/



{{ config(materialized='table', schema='marts') }}

WITH snapshot_data AS (
  SELECT 
    s.*,
    l.sk_location,
    jd.date_sk as sk_join_date,
    bd.date_sk as sk_birth_date,
  FROM {{ ref('customer_snapshot') }} s
  LEFT JOIN {{ ref('dim_location') }} l 
    ON upper(trim(l.country_name)) = upper(trim(s.home_country)) 
    AND upper(trim(l.region_name)) = upper(trim(s.home_location))
  LEFT JOIN {{ ref('dim_date') }} jd ON jd.date_day = s.first_join_date
  LEFT JOIN {{ ref('dim_date') }} bd ON bd.date_day = s.birth_date
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} AS sk_customer,
  customer_id,
  first_name, 
  last_name, 
  username, 
  email, 
  gender,
  sk_location, 
  sk_birth_date, 
  sk_join_date,
  dbt_valid_from,
  COALESCE(dbt_valid_to, '9999-12-31'::DATE) AS valid_to,
  dbt_valid_to IS NULL AS is_current
FROM snapshot_data
