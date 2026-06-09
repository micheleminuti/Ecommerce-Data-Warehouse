

{{ config(materialized='table', schema='marts') }}

WITH snapshot_data AS (
  SELECT 
    s.*,
    jd.sk_date as sk_join_date,
    bd.sk_date as sk_birth_date,
  FROM {{ ref('customer_snapshot') }} s
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
  home_location,
  home_country,
  sk_birth_date, 
  sk_join_date,
  s.dbt_valid_from,
  COALESCE(s.dbt_valid_to, '9999-12-31'::DATE) AS valid_to, 
  s.dbt_valid_to IS NULL AS is_current
FROM snapshot_data s
