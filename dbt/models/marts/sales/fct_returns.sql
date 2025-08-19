-- models/marts/fct_returns.sql
{{ config(materialized='table') }}

select
    r.product_key,
    r.territory_key,
    r.return_date,
    sum(r.return_quantity) as total_return_quantity

from {{ ref('stg_returns') }} r
group by 1, 2, 3