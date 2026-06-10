{% snapshot product_snapshot %}

{{ config(
  target_schema='snapshots',
  unique_key='product_id',
  strategy='check',
  check_cols=[
    'master_category', 'sub_category', 'article_type', 'gender', 'base_colour',
    'season', 'usage', 'product_display_name', 'year_of_production'
  ]
) }}

SELECT
  product_id,
  master_category,
  sub_category,
  article_type,
  gender,
  base_colour,
  season,
  usage,
  product_display_name,
  year_of_production
FROM {{ ref('product_ods') }}

{% endsnapshot %}
