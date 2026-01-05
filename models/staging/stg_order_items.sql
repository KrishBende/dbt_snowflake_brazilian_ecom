with source as (

    select *
    from {{ source('brazilian_ecommerce', 'order_items') }}

),

cleaned as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        cast(price as float) as price,
        cast(freight_value as float) as freight_value
    from source

)

select *
from cleaned