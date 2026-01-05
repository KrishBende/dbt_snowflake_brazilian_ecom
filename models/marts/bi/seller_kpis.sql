select
    s.seller_id,
    s.seller_state,

    s.total_orders,
    s.total_revenue,
    s.avg_review_score

from {{ ref('dim_sellers') }} s