with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__customer') }}

),

renamed as (

    select
        id,
        customer_types_id,
        name,
        name_fantasy,
        responsible,
        email,
        email2,
        phone,
        phone2,
        phone3,
        phone4,
        identification,
        ie,
        rg,
        birth_date,
        shipping_date,
        marital_state_id,
        gender_id,
        occupation,
        father_name,
        mother_name,
        origem,
        question_customer,
        question_whatsapp,
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
