-- this query finds the duplicate user IDs

with dim as (

    select * from {{ ref('dim_user_and_first_order') }}
)

select 
    user_id, 
    count(user_id) as user_id_count,

from dim
group by user_id
having count(user_id) > 1 