{{config(
    materialized='view'
)}}

with source as (
    select * from {{ source('raw', 'ADVENTUREWORKS_CUSTOMERS') }}
),

renamed as (
    select
        "CustomerKey" as customer_key,
        "Prefix" as prefix,
        "FirstName" as first_name,
        "LastName" as last_name,
        try_cast("BirthDate" as date) as birth_date,
        "MaritalStatus" as marital_status,
        "Gender" as gender,
        "EmailAddress" as email_address,
        try_cast(replace(replace("AnnualIncome", '$', ''), ',', '') as float) as annual_income,
        "TotalChildren" as total_children,
        "EducationLevel" as education_level,
        "Occupation" as occupation,
        "HomeOwner" as home_owner
            
    from source
)

select * from renamed