{{ config(
    materialized='incremental',
    unique_key='order_number'
) }}

with stg_sa as (
    select * 
    from {{ ref('stg_sales') }}
    {% if is_incremental() %}
        where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

stg_prod as (
    select * from {{ ref('stg_products') }}
),

stg_cust as (
    select * from {{ ref('stg_customers') }}
)

select 
    sa.order_number,
    sa.order_date,
    sa.customer_key,
    sa.product_key,
    sa.order_quantity as total_quantity,
    sa.order_quantity * p.price as total_revenue,
    coalesce(c.first_name, 'Unknown') as first_name,
    coalesce(p.product_name, 'Unknown') as product_name

from stg_sa sa 
left join stg_prod p on sa.product_key = p.product_key
left join stg_cust c on sa.customer_key = c.customer_key