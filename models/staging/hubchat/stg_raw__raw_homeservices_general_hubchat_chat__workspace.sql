with 

source as (

    select * from {{ source('raw', 'raw_homeservices_general_hubchat_chat__workspace') }}

),

renamed as (

    select
        __v,
        _id,
        active,
        assistant_id,
        assistant_name,
        lake_created_at,
        token,
        waba_id,
        workspace_id,
        month,
        day

    from source
    where active = true
    and assistant_name is not null
)

select distinct 
    token
    , assistant_name 
from renamed
