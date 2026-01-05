with source as (

    select *
    from {{ source('brazilian_ecommerce', 'order_payments') }}

),

cleaned as (

    select
        order_id,
        payment_sequential,
        lower(payment_type) as payment_type,
        payment_installments,
        cast(payment_value as float) as payment_value
    from source

)

select *
from cleaned