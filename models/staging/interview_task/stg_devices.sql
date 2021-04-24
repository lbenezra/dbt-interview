with source as (

    select * from {{ source('interview_task', 'devices') }}

),

staging_devices as (
   
   select 
   type_id,
   type,
   created_at,
   device as purchase_device,
   case when device = 'web'
            then 'desktop'
        when device in (
            'ios-app',
            'android-app',
            'android'
            )
            then 'mobile_app'
        when device in (
            'mobile',
            'tablet'
            )
            then 'mobile_web'
        when device is null
            then 'unknown'
        else 'error'
    end as purchase_device_type
   
   from source

)

select * from staging_devices
