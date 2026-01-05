with orders as (

    select *
    from {{ ref('fct_orders') }}
    where order_status = 'delivered'

),

final as (

    select
        date_trunc('day', order_purchase_timestamp) as order_date,

        count(distinct order_id) as total_orders,
        sum(total_payment_value) as total_revenue,
        avg(total_payment_value) as avg_order_value,
        avg(total_items) as avg_items_per_order,

        avg(delivery_days) as avg_delivery_days,

        avg(delivered_on_time) * 100
            as on_time_delivery_pct,

        avg(review_score) as avg_review_score

    from orders
    group by 1

)

select *
from final