with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_growth_history') }}

),

renamed as (

    select
        id,
        type,
        line_id,
        queue_id,
        username_update,
        midia,
        id_campanha,
        campanha,
        fonte,
        pagina,
        dominio,
        destino,
        operacao,
        growth,
        category_id,
        inactive_date,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key,
        brand,
        grupo_anuncio,
        grupo_utm,
        termo_utm

    from source

)

select 
    line_id,
    if(campanha = "", null, campanha) campanha,
    pagina,
    fonte,
    midia,
    brand,
    id
from renamed
