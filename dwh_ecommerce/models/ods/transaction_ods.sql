{{ config(
    materialized='table',
    schema='ods'
)}}

with source as (
    select * from {{ ref('transactions') }}
),

pre_cleaned as (
    select
        -- Gestione Timestamps (Separazione Data e Ora)
        created_at::timestamp as created_at,
        shipment_date_limit::timestamp as shipment_date_limit,
        
        customer_id::bigint as customer_id,
        booking_id as booking_id,
        
        -- Pulizia JSON
        replace(product_metadata, '''', '"')::json as metadata_json,
        
        -- Standardizzazione testi e logica booleana
        trim(payment_method) as payment_method,
        case 
            when trim(upper(payment_status)) = 'SUCCESS' then true 
            else false 
        end as is_payment_success,
        
        -- Gestione Promo Code (Null/Vuoti -> NO CODE)
        coalesce(nullif(trim(promo_code), ''), 'NO CODE') as promo_code,

        -- Valori monetari originali (Double)
        promo_amount::double as promo_amount,
        shipment_fee::double as shipment_fee
        
    from source
    where customer_id is not null
      and created_at is not null
),

flattened as (
    select
        c.*,
        unnest(json_transform(metadata_json, '[{"product_id": "BIGINT", "quantity": "INTEGER", "item_price": "DOUBLE"}]')) as product_struct
    from pre_cleaned c
)

select
    -- Identificativi e Metadati
    customer_id,
    booking_id,
    product_struct.product_id,

    payment_method,
    is_payment_success,
    promo_code,

    -- Espansione Prodotto
    product_struct.quantity,

    created_at,
    shipment_date_limit,

    -- Total Amount in Euro (quantity * item_price / 17000)
    (product_struct.quantity * product_struct.item_price / 17000)::decimal(18,2) as total_amount_eur,

    -- Conversioni in Euro (Valore / 17000) per promo e shipment
    (promo_amount / 17000)::decimal(18,2) as promo_amount_eur,
    (shipment_fee / 17000)::decimal(18,2) as shipment_fee_eur,


    -- Total Amount originale IDR (quantity * item_price)
    (product_struct.quantity * product_struct.item_price) as total_amount_idr,
    promo_amount as promo_amount_idr,
    shipment_fee as shipment_fee_idr

    

from flattened
