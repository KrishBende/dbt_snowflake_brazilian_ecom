with source as (

    select * 
    from {{ source('brazilian_ecommerce', 'customers') }}

),

cleaned as (

    select
        customer_id,
        customer_unique_id,
        cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
        initcap(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state
    from source

)

select *
from cleaned