with sellers as (

    select distinct
        seller_id,
        seller_city,
        seller_state
    from {{ ref('stg_sellers') }}

),

seller_order_metrics as (

    select
        seller_id,
        count(distinct order_id) as total_orders,
        sum(price) as total_revenue
    from {{ ref('stg_order_items') }}
    group by seller_id

),

seller_review_metrics as (

    select
        oi.seller_id,
        avg(r.review_score) as avg_review_score
    from {{ ref('stg_order_items') }} oi
    left join {{ ref('stg_order_reviews') }} r
        on oi.order_id = r.order_id
    group by oi.seller_id

),

final as (

    select
        s.seller_id,
        s.seller_city,
        s.seller_state,

        coalesce(so.total_orders, 0) as total_orders,
        coalesce(so.total_revenue, 0) as total_revenue,
        coalesce(sr.avg_review_score, 0) as avg_review_score

    from sellers s
    left join seller_order_metrics so
        on s.seller_id = so.seller_id
    left join seller_review_metrics sr
        on s.seller_id = sr.seller_id

)

select *
from final