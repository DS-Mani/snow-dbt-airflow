{{config(materalized = 'table')}}

with returns as (
    select * from {{ ref('stg_returns') }}
),

products as (
    select product_key, product_name, color, size
    from {{ ref('stg_products') }}
),

territories as (
    select territory_key, region
    from {{ ref('stg_territories') }}
),

final as (
    select
        r.return_date,
        r.product_key,
        p.product_name,
        p.color,
        p.size,
        r.territory_key,
        t.region,
        r.return_quantity
    from returns r
    left join products p on r.product_key = p.product_key
    left join territories t on r.territory_key = t.territory_key
)

select * from final