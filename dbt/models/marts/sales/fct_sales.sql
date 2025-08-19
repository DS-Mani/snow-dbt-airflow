{{ config(materialized='table') }}

select
     product_key,
    customer_key,
    territory_key,
    order_date,
    
    sum(order_quantity) as total_quantity,
    sum(order_quantity * price) as total_revenue

from {{ ref('int_sales_with_details') }}
group by 1, 2, 3, 4