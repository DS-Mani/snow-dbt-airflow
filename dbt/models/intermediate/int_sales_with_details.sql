{{ config(materialized='view') }}

with sales as (
    select * from {{ ref('stg_sales') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

calendar as (
    select * from {{ ref('stg_calendar') }}
),
territories as (
    select * from {{ ref('stg_territories') }}
),

final as (
    select
        
        s.order_date,
        s.stock_date,
        s.order_number,
        s.order_line_item,
        s.order_quantity,

        c.customer_key,
        c.first_name,
        c.last_name,
        c.gender,
        c.annual_income,

        p.product_key,
        p.product_name,
        p.color,
        p.size,
        p.style,
        p.price,

        cal.date as order_calendar_date,
        
        s.territory_key,


        t.region,
        t.country,
        t.continent,



    from sales s
    left join products p on s.product_key = p.product_key
    left join customers c on s.customer_key = c.customer_key
    left join calendar cal on s.order_date = cal.date
    left join territories t on s.territory_key = t.territory_key
)

select * from final