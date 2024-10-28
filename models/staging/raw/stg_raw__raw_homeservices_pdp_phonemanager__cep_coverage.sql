with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__cep_coverage') }}

),

renamed as (

    select
        id,
        cod_ibge,
        city_name,
        state,
        cep,
        group,
        value_billet,
        value_ccard,
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
