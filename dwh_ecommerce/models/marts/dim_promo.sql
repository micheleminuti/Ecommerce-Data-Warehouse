{{ config(
    materialized='table',
    schema='marts'
) }}



with raw_promos as (  
    select distinct 
        promo_code
    from {{ ref('transaction_ods') }}  
) 



select 
    row_number() over (order by promo_code) as sk_promo,
    promo_code
from raw_promos  
