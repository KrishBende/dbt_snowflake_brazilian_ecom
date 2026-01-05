with source as (

    select *
    from {{ source('brazilian_ecommerce', 'geolocation') }}

),

cleaned as (

    select
        cast(geolocation_zip_code_prefix as integer) as zip_code_prefix,
        cast(geolocation_lat as float) as latitude,
        cast(geolocation_lng as float) as longitude,
        initcap(trim(geolocation_city)) as city,
        upper(trim(geolocation_state)) as state
    from source

)

select *
from cleaned
