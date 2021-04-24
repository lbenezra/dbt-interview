with orders as (

    select * from {{ ref('stg_orders') }}

),

addresses as (

    select * from {{ ref('stg_addresses') }}
),

joined as (

    select 
        addresses.user_id,
        min(orders.order_id) as first_order_id,
        case when min(orders.order_id) = orders.order_id
                then 'new'
             else 'repeat'
        end as user_type,
        addresses.name,
        addresses.address,
        addresses.state,
        addresses.country_code,
        addresses.country_type,

    from orders
    left join 
    addresses on addresses.order_id = orders.order_id
    where order_status != 'cancelled'
    group by user_id, orders.order_id, name, address, state, country_code, country_type

)

select * from joined
