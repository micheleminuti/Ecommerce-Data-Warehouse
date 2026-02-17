{% snapshot customer_snapshot %}

{{ config(
  target_schema='snapshots',
  unique_key='customer_id',
  strategy='check',
  check_cols=[
    'first_name', 'last_name', 'username', 'email', 'gender', 'birth_date',
    'home_country', 'home_location'
  ]
) }}

SELECT 
  customer_id,
  trim(first_name) as first_name,
  trim(last_name) as last_name,
  trim(username) as username,
  trim(lower(email)) as email,
  upper(gender) as gender,
  birthdate::date as birth_date,
  trim(upper(home_location)) as home_location,
  trim(upper(home_country)) as home_country,
  first_join_date::date as first_join_date,
FROM {{ ref('customers') }}
WHERE customer_id IS NOT NULL 
  AND length(trim(email)) > 0 
  AND gender IN ('M', 'F')

{% endsnapshot %}
