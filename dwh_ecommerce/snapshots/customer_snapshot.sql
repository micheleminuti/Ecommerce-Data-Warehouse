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
  first_name,
  last_name,
  username,
  email,
  gender,
 birth_date,
  home_location,
  home_country,
  first_join_date
FROM {{ ref('customer_ods') }}



{% endsnapshot %}
