with 

source as (

    select * from {{ source('raw', 'raw_finance_general_hubchat_escale_finance__workspace') }}

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
        workspace_id,
        year,
        month,
        day

    from source

)

select * from renamed
