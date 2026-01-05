select
    f.*,
    l.is_churned
from {{ ref('customer_churn_features') }} f
join {{ ref('customer_churn_labels') }} l
    on f.customer_id = l.customer_id