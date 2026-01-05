with products as (

    select *
    from {{ ref('stg_products') }}

),

product_metrics as (

    select
        oi.product_id,
        count(oi.order_id) as total_orders,
        sum(oi.price) as total_revenue,
        avg(oi.price) as avg_price
    from {{ ref('stg_order_items') }} oi
    group by oi.product_id

),

final as (

    select
        p.product_id,
        p.product_category_name,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        coalesce(pm.total_orders, 0) as total_orders,
        coalesce(pm.total_revenue, 0) as total_revenue,
        coalesce(pm.avg_price, 0) as avg_price

    from products p
    left join product_metrics pm
        on p.product_id = pm.product_id

)

select *
from final