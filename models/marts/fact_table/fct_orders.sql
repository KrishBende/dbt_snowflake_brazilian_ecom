with orders as (

    select *
    from {{ ref('stg_orders') }}

),

order_items as (

    select
        order_id,
        count(order_item_id) as total_items,
        sum(price) as total_item_value,
        sum(freight_value) as total_freight_value
    from {{ ref('stg_order_items') }}
    group by order_id

),

payments as (

    select
        order_id,
        sum(payment_value) as total_payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id

),

reviews as (

    select
        order_id,
        review_score
    from {{ ref('stg_order_reviews') }}

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,

        coalesce(oi.total_items, 0) as total_items,
        coalesce(oi.total_item_value, 0) as total_item_value,
        coalesce(oi.total_freight_value, 0) as total_freight_value,
        coalesce(p.total_payment_value, 0) as total_payment_value,

        r.review_score,

        datediff(
            day,
            o.order_purchase_timestamp,
            o.order_delivered_customer_date
        ) as delivery_days,

        case
            when o.order_delivered_customer_date
                 <= o.order_estimated_delivery_date
            then 1 else 0
        end as delivered_on_time

    from orders o
    left join order_items oi on o.order_id = oi.order_id
    left join payments p on o.order_id = p.order_id
    left join reviews r on o.order_id = r.order_id

)

select *
from final