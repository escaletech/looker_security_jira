with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_search_console__pages') }}

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
        query,
        page,
        month,
        day

    from source

)

select * from renamed
