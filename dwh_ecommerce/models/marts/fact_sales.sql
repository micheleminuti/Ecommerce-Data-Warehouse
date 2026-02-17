


{{ config(
    materialized='incremental',
    schema='marts',
    unique_key=['fk_customer', 'fk_date', 'fk_time', 'fk_device', 'fk_promo', 'fk_payment', 'fk_shipment_date_limit', 'fk_product'],
    incremental_strategy='delete+insert'
)}}




with transaction_base as (
    select
        t.customer_id as customer_id, 
        t.booking_id as booking_id, 
        t.payment_method as payment_method, 
        t.is_payment_success as is_payment_success, 
        t.promo_code as promo_code,
        t.created_at as created_at, 
        t.shipment_date_limit as shipment_date_limit, 
        t.promo_amount_eur as promo_amount_eur, 
        t.promo_amount_idr as promo_amount_idr,
        t.shipment_fee_eur as shipment_fee_eur, 
        t.shipment_fee_idr as shipment_fee_idr, 
        t.product_id as product_id, 
        t.quantity as quantity, 
        t.total_amount_eur as total_amount_eur, 
        t.total_amount_idr as total_amount_idr,
        
        -- Da customer_ods
        c.device_type as device_type,
        c.device_version as device_version
        
    from {{ ref('transaction_ods') }} t
    left join {{ ref('customer_ods') }} c using (customer_id)  -- ✅ Join su customer_id
    where t.is_payment_success = true
),




enriched_transactions as (
    select 
        tb.*,
        extract(hour from tb.created_at)::int as ora,
        extract(minute from tb.created_at)::int as minuto,
        
        -- FK rapidi
        dc.sk_customer,
        dd_date.date_sk as sk_date,
        dt.sk_hour as sk_time,
        dp.sk_promo,
        dproduct.sk_product,
        dpay.sk_payment,
        dd_shipment.date_sk as sk_shipment_date_limit,
        ddev.sk_device  -- Già nel dim_customer!
        
    from transaction_base tb
    
    left join {{ ref('dim_customer') }} dc 
        ON dc.customer_id = tb.customer_id
        AND cast(tb.created_at as date) BETWEEN dc.dbt_valid_from AND dc.valid_to  

    left join {{ ref('dim_date') }} dd_date on dd_date.date_day = cast(tb.created_at as date)

    left join {{ ref('dim_time') }} dt on dt.ora = extract(hour from tb.created_at)::int 
                                      and dt.minuto = extract(minute from tb.created_at)::int

    left join {{ ref('dim_promo') }} dp using (promo_code)

    left join {{ ref('dim_product') }} dproduct using (product_id)

    left join {{ ref('dim_payment') }} dpay using (payment_method, is_payment_success)

    left join {{ ref('dim_date') }} dd_shipment on dd_shipment.date_day = cast(tb.shipment_date_limit as date)

    left join {{ ref('dim_device') }}   ddev
           on  ddev.device_type   = tb.device_type
           and ddev.device_version = tb.device_version
)


select 
    sk_customer as fk_customer, 
    sk_date as fk_date, 
    sk_time as fk_time, 
    sk_device as fk_device, 
    sk_promo as fk_promo, 
    sk_payment as fk_payment, 
    sk_shipment_date_limit as fk_shipment_date_limit, 
    sk_product as fk_product,
    
    quantity,
    total_amount_eur,
    total_amount_idr,
    shipment_fee_eur,
    promo_amount_eur,
    promo_amount_idr

from enriched_transactions

