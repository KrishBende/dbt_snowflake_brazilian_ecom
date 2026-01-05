with orders as (

    select
        customer_id,
        order_id,
        order_purchase_timestamp::date as order_date,
        total_payment_value,
        review_score,
        delivery_days
    from {{ ref('fct_orders') }}

),

agg as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as last_order_date,

        count(order_id) as total_orders,
        sum(total_payment_value) as lifetime_value,
        avg(total_payment_value) as avg_order_value,
        avg(review_score) as avg_review_score,
        avg(delivery_days) as avg_delivery_days

    from orders
    group by customer_id

),

final as (

    select
        customer_id,
        total_orders,
        lifetime_value,
        avg_order_value,
        avg_review_score,
        avg_delivery_days,

        datediff(day, first_order_date, last_order_date) as customer_lifetime_days
    from agg
)

select * from final