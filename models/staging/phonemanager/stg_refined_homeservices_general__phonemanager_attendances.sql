with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_attendances') }}

),

renamed as (

    select
        id,
        user_id,
        user_in_attendance,
        team_id,
        customer_id,
        address_billing_id,
        address_delivery_id,
        address_sent_id,
        protocol,
        status_id,
        initial_stage_screen,
        type_id,
        type_non_sale_id,
        level_interest,
        payment_adhesion_id,
        adhesion_value,
        payment_monthly_id,
        monthly_value,
        reason,
        active,
        total_pontos,
        manual,
        category_id,
        audit,
        date_sale,
        date_nonsale,
        date_audit,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key,
        brand

    from source
    where deleted_at IS NULL 

)

select 
    id,
    protocol, 
    user_id,
    team_id,
    date_sale,
    brand,
    created_at,
    date_nonsale,
    type_id,
    customer_id,
    manual,
    status_id,
    payment_monthly_id,
    type_non_sale_id
from renamed
