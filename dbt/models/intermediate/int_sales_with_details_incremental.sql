{{ config(
    materialized='incremental',
    unique_key='order_number',
    incremental_strategy='merge'
) }}

with stg_sa as (
    select * from {{ ref('stg_sales') }}

    {% if is_incremental() %}
      where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

stg_cust as (
    select * from {{ ref('stg_customers') }}
),

stg_prod as (
    select * from {{ ref('stg_products') }}
)

select
    sa.order_number,
    sa.order_date,
    sa.customer_key,
    sa.product_key,
    sa.order_quantity,
    sa.order_quantity * prod.price as revenue,
    coalesce(cust.first_name, 'Unknown') as customer_name,
    coalesce(prod.product_name, 'Unknown') as product_name

from stg_sa sa
left join stg_cust cust on sa.customer_key = cust.customer_key
left join stg_prod prod on sa.product_key = prod.product_key