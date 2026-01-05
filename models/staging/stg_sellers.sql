with source as (

    select *
    from {{ source('brazilian_ecommerce', 'sellers') }}

),

cleaned as (

    select
        seller_id,
        cast(seller_zip_code_prefix as integer) as seller_zip_code_prefix,
        initcap(trim(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state
    from source

)

select *
from cleaned