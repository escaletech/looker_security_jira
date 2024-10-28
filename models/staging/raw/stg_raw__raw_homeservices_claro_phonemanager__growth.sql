with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__growth') }}

),

renamed as (

    select
        id,
        type,
        line_id,
        queue_id,
        midia,
        id_campanha,
        campanha,
        fonte,
        pagina,
        grupo_anuncio,
        grupo_utm,
        termo_utm,
        dominio,
        destino,
        operacao,
        growth,
        category_id,
        active,
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