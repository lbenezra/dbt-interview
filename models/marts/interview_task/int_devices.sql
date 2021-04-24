with devices as (

    select * from {{ ref('stg_devices') }}

),


rename_devices as (

    select 
        distinct cast (type_id as int64) as order_id,
        first_value(purchase_device) over(
            partition by type_id
            order by
                created_at rows between unbounded preceding and unbounded following
        ) as device,
        purchase_device_type,
        created_at
    from devices
    where type = 'order'  
)

select * from rename_devices