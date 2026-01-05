with customers as (

    select *
    from {{ ref('stg_customers') }}

),

customer_metrics as (

    select
        customer_id,
        count(order_id) as total_orders,
        sum(total_payment_value) as lifetime_value,
        avg(total_payment_value) as avg_order_value
    from {{ ref('fct_orders') }}
    group by customer_id

),

final as (

    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,

        coalesce(cm.total_orders, 0) as total_orders,
        coalesce(cm.lifetime_value, 0) as lifetime_value,
        coalesce(cm.avg_order_value, 0) as avg_order_value

    from customers c
    left join customer_metrics cm
        on c.customer_id = cm.customer_id

)

select *
from final