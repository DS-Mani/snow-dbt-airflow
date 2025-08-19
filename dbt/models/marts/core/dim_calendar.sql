{{ config(materialized='table') }}

select
    "Date" as date_day,
    to_date("Date") as date_actual,
    extract(year from to_date("Date")) as year,
    extract(month from to_date("Date")) as month,
    extract(day from to_date("Date")) as day,
    extract(quarter from to_date("Date")) as quarter

from {{ source('raw', 'ADVENTUREWORKS_CALENDAR') }}