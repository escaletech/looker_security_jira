with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_search_console__only_pages') }}

),

renamed as (

    select
        clicks,
        ctr,
        domain,
        impressions,
        lake_created_at,
        position,
        name,
        date,
        page,
        month,
        day

    from source

)

select * from renamed
