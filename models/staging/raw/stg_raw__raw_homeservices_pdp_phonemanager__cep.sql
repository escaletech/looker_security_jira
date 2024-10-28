with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__cep') }}

),

renamed as (

    select
        id,
        ibge,
        cep,
        state_uf,
        city,
        district,
        street,
        complement,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
