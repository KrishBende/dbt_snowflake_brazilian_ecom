with source as (

    select *
    from {{ source('brazilian_ecommerce', 'product_category_name_translation') }}

),

cleaned as (

    select
        c1 as product_category_name,
        c2 as product_category_name_english
    from source

)

select *
from cleaned