{{ config(
    materialized='table',
    schema='marts'
) }}



with raw_payments as (  
    select distinct 
        payment_method,
        is_payment_success
    from {{ ref('transaction_ods') }}  
) 

select 
    row_number() over (order by payment_method, is_payment_success) as sk_payment,
    payment_method,
    is_payment_success
from raw_payments  
