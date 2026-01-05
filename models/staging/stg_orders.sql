with source as (

    select *
    from {{ source('brazilian_ecommerce', 'orders') }}

),

cleaned as (

    select
        order_id,
        customer_id,
        lower(order_status) as order_status,

        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from source
    where order_purchase_timestamp is not null

)

select *
from cleaned