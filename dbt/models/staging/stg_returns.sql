{{ config(materialized='view') }}

select
    try_cast("ReturnDate" as date) as return_date,
    "TerritoryKey" as territory_key,
    "ProductKey" as product_key,
    "ReturnQuantity" as return_quantity

from {{ source('raw', 'ADVENTUREWORKS_RETURNS') }}