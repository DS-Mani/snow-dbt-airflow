{{ config(materialized='table') }}

select
    customer_key,
    prefix,
    first_name,
    last_name,
    birth_date,
    gender,
    marital_status,
    email_address,
    annual_income,
    total_children,
    education_level,
    occupation,
    home_owner
from {{ ref('stg_customers') }}