with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_escale_messages') }}

),

renamed as (

    select
        __v
        ,_id
        ,data
        ,day
        ,lake_created_at
        ,lake_key
        ,month
        ,options
        ,response
        ,response_title
        ,response_type
        ,session_init
        ,status
        ,template_name
        ,text
        ,timestamp
        ,token
        ,who
        ,wpp_body
        ,year

    from source

)

select 
    /*session_init message_session_id
    ,token
    ,user_id
    ,timestamp tsp_message
    ,who desc_message_source
    ,message_status desc_message_status
    ,text desc_message_text
    ,'' as response_type
    ,'' as response_source*/
    response_title
    ,status
    ,template_name
 from renamed
