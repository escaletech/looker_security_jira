with 

source as (

    select * from {{ source('trusted_finance_general', 'hubchat_escale_finance_workspace') }}

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
