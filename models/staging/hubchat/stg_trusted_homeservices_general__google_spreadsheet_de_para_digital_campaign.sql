with 

source as (

    select * from {{ source('trusted_homeservices_general', 'google_spreadsheet_de_para_digital_campaign') }}

),

renamed as (

    select
        campanha,
        codigo,
        medium,
        source,
        lake_created_at,
        year,
        month,
        day,
        lake_key

    from source
    union ALL
        select
        'SEM CAMPANHA' AS campanha,
        'SEM INFORMAÇAO' AS codigo,
        'SEM INFORMAÇAO' AS medium,
        'SEM INFORMAÇAO' AS source,
        NULL lake_created_at,
        NULL year,
        NULL month,
        NULL day,
        NULL lake_key

)

select 
    codigo AS digital_campaing_id
    ,* 
from renamed
