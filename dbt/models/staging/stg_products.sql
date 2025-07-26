{{ config(materialized='view') }}

select
    "ProductKey" as product_key,
    "ProductSubcategoryKey" as product_subcategory_key,
    "ProductSKU" as stock_keeping_unit,
    "ProductName" as product_name,
    "ModelName" as model_name,
    "ProductDescription" as description,
    "ProductColor" as color,
    "ProductSize" as size,
    "ProductStyle" as style,
    try_cast("ProductCost" as float) as cost,
    try_cast("ProductPrice" as float) as price

from {{ source('raw', 'ADVENTUREWORKS_PRODUCTS') }}