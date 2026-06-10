{{ config(
    materialized='table',
    schema='marts'
) }}


with snapshot_data as (
    select *
    from {{ ref('product_snapshot') }}
)

        SELECT
            {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} AS sk_product,
            product_id,
            master_category,
            sub_category,
            article_type,
            gender,
            base_colour,
            season,
            usage,
            product_display_name,
            year_of_production,
            s.dbt_valid_from,
            COALESCE(s.dbt_valid_to, '9999-12-31'::DATE) AS valid_to,
            s.dbt_valid_to IS NULL AS is_current
        FROM snapshot_data s
