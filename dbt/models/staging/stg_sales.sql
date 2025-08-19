{{ config(materialized='view') }}

with base as (

    select * from {{ source('raw', 'ADVENTUREWORKS_SALES_2015') }}
    union all
    select * from {{ source('raw', 'ADVENTUREWORKS_SALES_2016') }}
    union all
    select * from {{ source('raw', 'ADVENTUREWORKS_SALES_2017') }}

),

final as (

    select
        try_cast("OrderDate" as date) as order_date,
        try_cast("StockDate" as date) as stock_date,
        "OrderNumber" as order_number,
        "ProductKey" as product_key,
        "CustomerKey" as customer_key,
        "TerritoryKey" as territory_key,
        "OrderLineItem" as order_line_item,
        "OrderQuantity" as order_quantity

    from base

)

select * from final