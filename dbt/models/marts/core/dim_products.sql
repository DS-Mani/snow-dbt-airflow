{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_products') }}
),

subcategories as (
    select
        "ProductSubcategoryKey" as product_subcategory_key,
        "SubcategoryName" as subcategory_name,
        "ProductCategoryKey" as product_category_key
    from {{ source('raw', 'ADVENTUREWORKS_PRODUCT_SUBCATEGORIES') }}
),

categories as (
    select
        "ProductCategoryKey" as product_category_key,
        "CategoryName" as category_name
    from {{ source('raw', 'ADVENTUREWORKS_PRODUCT_CATEGORIES') }}
),

final as (
    select
        p.product_key,
        p.stock_keeping_unit,
        p.product_name,
        p.model_name,
        p.description,
        p.color,
        p.size,
        p.style,
        p.cost,
        p.price,
        s.subcategory_name,
        c.category_name

    from products p
    left join subcategories s on p.product_subcategory_key = s.product_subcategory_key
    left join categories c on s.product_category_key = c.product_category_key
)

select * from final