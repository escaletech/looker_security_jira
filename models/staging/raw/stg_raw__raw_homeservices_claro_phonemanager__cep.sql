with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__cep') }}

),

renamed as (

    select
        id,
        ibge,
        cep,
        regional,
        state_uf,
        name_uf,
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
