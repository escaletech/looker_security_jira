with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__telephony_types') }}

),

renamed as (

    select
        id,
        description,
        reference,
        pattern,
        active,
        show,
        finalize,
        transfer,
        types,
        reports,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
