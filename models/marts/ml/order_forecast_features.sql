with daily_kpis as (

    select
        order_date,
        total_revenue,
        total_orders,
        avg_order_value,
        avg_items_per_order,
        avg_delivery_days,
        avg_review_score
    from {{ ref('fct_orders_kpis') }}

),

features as (

    select
        order_date,

        total_revenue,
        total_orders,
        avg_order_value,
        avg_items_per_order,
        avg_delivery_days,
        avg_review_score,

        -- lag features
        lag(total_orders, 1) over (order by order_date) as order_lag_1,
        lag(total_orders, 7) over (order by order_date) as order_lag_7,

        -- rolling features
        avg(total_orders) over (
            order by order_date
            rows between 7 preceding and 1 preceding
        ) as order_7d_avg

    from daily_kpis

)

select *
from features
where order_7d_avg is not null