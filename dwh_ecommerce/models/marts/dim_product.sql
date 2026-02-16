{{ config(
    materialized='table',
    schema='marts'
) }}


with raw_products as (  
    select distinct 
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
    from {{ ref('product_ods') }}  
) 


select 
    row_number() over (order by product_id) as sk_product,
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
from raw_products
