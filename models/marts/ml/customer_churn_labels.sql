with orders as (

    select
        customer_id,
        order_purchase_timestamp::date as order_date
    from {{ ref('stg_orders') }}

),

last_order as (

    select
        customer_id,
        max(order_date) as last_order_date
    from orders
    group by customer_id

),

labels as (

    select
        customer_id,
        last_order_date,

        case
            when last_order_date < dateadd(day, -90, max(last_order_date) over ())
                then 1
            else 0
        end as is_churned

    from last_order

)

select * from labels