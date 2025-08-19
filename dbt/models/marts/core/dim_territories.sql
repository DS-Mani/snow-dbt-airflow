{{ config(materialized='table') }}

select
     territory_key ,
    continent,
    country,
    region
from {{ ref('stg_territories') }}