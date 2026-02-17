{{ config(
    materialized='incremental',
    schema='marts',
    unique_key=['sk_customer', 'sk_date', 'sk_time', 'sk_device', 'sk_promo', 'sk_payment', 'sk_shipment_date_limit', 'sk_product'],
    incremental_strategy='delete+insert'
)}}



with transaction_base as (
    select
        customer_id, booking_id, payment_method, is_payment_success, promo_code,
        created_at, shipment_date_limit, promo_amount_eur, promo_amount_idr,
        shipment_fee_eur, shipment_fee_idr, product_id, quantity, 
        total_amount_eur, total_amount_idr
        
    from {{ ref('transaction_ods') }}
    where is_payment_success = true

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
        dc.sk_device  -- Già nel dim_customer!
        
    from transaction_base tb
    
    left join {{ ref('dim_customer') }} dc using (customer_id)
    left join {{ ref('dim_date') }} dd_date on dd_date.date_day = cast(tb.created_at as date)
    left join {{ ref('dim_time') }} dt on dt.ora = extract(hour from tb.created_at)::int 
                                      and dt.minuto = extract(minute from tb.created_at)::int
    left join {{ ref('dim_promo') }} dp using (promo_code)
    left join {{ ref('dim_product') }} dproduct using (product_id)
    left join {{ ref('dim_payment') }} dpay using (payment_method, is_payment_success)
    left join {{ ref('dim_date') }} dd_shipment on dd_shipment.date_day = cast(tb.shipment_date_limit as date)
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
    
    count(*) as order_count,
    sum(quantity) as quantity,
    sum(total_amount_eur) as incasso_eur,
    sum(total_amount_idr) as incasso_idr,
    sum(shipment_fee_eur) as shipment_fee_eur,
    sum(shipment_fee_idr) as shipment_fee_idr,
    sum(promo_amount_eur) as promo_amount_eur,
    sum(promo_amount_idr) as promo_amount_idr

from enriched_transactions
where sk_customer is not null and sk_date is not null and sk_product is not null
group by 1,2,3,4,5,6,7,8
