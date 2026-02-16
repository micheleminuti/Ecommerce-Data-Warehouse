
{{ config(
    materialized='table',
    schema='marts'
) }}

with raw_locations as (  
    select distinct 
        home_country as country_name,
        home_location as region_name
    from {{ ref('customer_ods') }}  
    where home_country is not null or home_location is not null
) 

select 
    row_number() over (order by country_name, region_name) as sk_location,
    country_name,
    region_name
from raw_locations  
