with source as (

    select * from {{ source('interview_task', 'addresses') }}

),

staging_addresses as (
    
    select 
    order_id,
    user_id,
    name,
    address,
    state,
    country_code,
    case when country_code is null 
            then 'NULL country'
         when country_code != 'US' 
            then 'International'
         else country_code
    end as country_type

    from source
)


select * from staging_addresses
