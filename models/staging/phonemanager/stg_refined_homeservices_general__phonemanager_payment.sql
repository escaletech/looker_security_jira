with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_payment') }}

),

renamed as (

    select
        id,
        attendance_id,
        digital_invoice,
        payment_type,
        payment_category,
        credit_card_company_id,
        card_number,
        card_security_code,
        card_validity,
        card_holder,
        bank,
        agency,
        agency_digit,
        account,
        account_digit,
        created_at,
        updated_at,
        deleted_at,
        json_payment,
        lake_created_at,
        year,
        month,
        day,
        lake_key,
        brand

    from source

)

select * from renamed
