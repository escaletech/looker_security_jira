with 

source as (

    select * from {{ source('raw', 'raw_finance_general_hubchat_escale_finance__attendance') }}
),

renamed as (

    select
      2 id

    from source

)

select * from renamed
