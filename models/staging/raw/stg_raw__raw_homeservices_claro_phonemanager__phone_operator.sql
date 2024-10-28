with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__phone_operator') }}

),

renamed as (

    select
        id,
        identification,
        type_id,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
