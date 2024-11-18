with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_chat_workspace') }}

)

,renamed as (

    select
        __v,
        _id,
        active,
        assistant_id,
        assistant_name,
        lake_created_at,
        token,
        workspace_id,
        year,
        month,
        day,
        lake_key

    from source

)

select distinct 
    token
    , assistant_name 
from renamed
