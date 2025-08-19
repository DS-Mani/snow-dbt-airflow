{{ config(materialized = 'view') }}

select 
    "Continent" as continent,
    "Country" as country,
    "Region" as region,
    "SalesTerritoryKey" as territory_key

from {{ source('raw', 'ADVENTUREWORKS_TERRITORIES') }}