{{ config(
    materialized='table',
    schema='marts'
) }}

with raw_devices as (  
    select distinct 
        device_type,
        device_version
    from {{ ref('customer_ods') }}  
) 

select 
    row_number() over (order by device_type, device_version) as sk_device,
    device_type,
    device_version
from raw_devices  
