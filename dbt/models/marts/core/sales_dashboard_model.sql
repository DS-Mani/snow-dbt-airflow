{{ config(materialized='table') }}

select
    fs.order_date,
    dc.customer_key,
    dc.first_name,
    dp.product_key,
    dp.product_name,
    dt.region,
    fs.total_quantity,
    fs.total_revenue

from {{ ref('fct_sales') }} fs
left join {{ ref('dim_customers') }} dc using (customer_key)
left join {{ ref('dim_products') }} dp using (product_key)
left join {{ ref('dim_territories') }} dt using (territory_key)
