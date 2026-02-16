{{ config(
    materialized='table',
    schema='ods'
) }}

with source as (
    select * from {{ ref('products') }}
),

cleaned as (
    select
        -- ids / keys
        id::bigint as product_id,

        -- text standardization
        trim(upper(gender)) as gender,  -- MEN/WOMEN/...
        trim(masterCategory) as master_category,
        trim(subCategory) as sub_category,
        trim(articleType) as article_type,
        trim(baseColour) as base_colour,
        trim(season) as season,
        year::int as year_of_production,
        trim(usage) as usage,
        trim(productDisplayName) as product_display_name

    from source
    where id is not null
      and length(trim(productDisplayName)) > 0
)

select * from cleaned
