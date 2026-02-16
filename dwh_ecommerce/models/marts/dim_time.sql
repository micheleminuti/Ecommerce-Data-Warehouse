{{ config(
    materialized='table',
    schema='marts'
) }}

select 
    row_number() over (order by ora, minuto) as sk_hour,
    ora,
    minuto,
    lpad(ora::varchar, 2, '0') || ':' || lpad(minuto::varchar, 2, '0') as time_str
from generate_series(0, 23) as ora(ora),
     generate_series(0, 59) as minuto(minuto)
