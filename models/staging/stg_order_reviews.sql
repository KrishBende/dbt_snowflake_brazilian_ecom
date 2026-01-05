with source as (

    select *
    from {{ source('brazilian_ecommerce', 'order_reviews') }}

),

ranked as (

    select
        review_id,
        order_id,
        cast(review_score as integer) as review_score,
        review_creation_date,
        review_answer_timestamp,

        row_number() over (
            partition by order_id
            order by review_creation_date desc
        ) as rn
    from source

)

select
    review_id,
    order_id,
    review_score,
    review_creation_date,
    review_answer_timestamp
from ranked
where rn = 1