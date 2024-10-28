with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_lead_platform__leads') }}

),

renamed as (

    select
        _id,
        brand,
        campaign_utm_campaign,
        campaign_utm_content,
        campaign_utm_medium,
        campaign_utm_source,
        campaign_utm_term,
        channel,
        contacts_emails_other,
        contacts_emails_primary,
        contacts_emails_secondary,
        contacts_phones_primary_ddd,
        contacts_phones_primary_phone,
        contacts_phones_secondary_ddd,
        contacts_phones_secondary_phone,
        created_at,
        created_by,
        crm_channels_hubspot_id,
        customer_addresses_primary_city,
        customer_addresses_primary_complement,
        customer_addresses_primary_district,
        customer_addresses_primary_number,
        customer_addresses_primary_state,
        customer_addresses_primary_street,
        customer_addresses_primary_zip_code,
        customer_cpf,
        customer_gender,
        customer_name,
        funnel_step,
        input_type,
        lake_created_at,
        origin,
        source,
        updated_at,
        updated_by,
        vertical,
        year,
        month,
        day

    from source

)

select * from renamed
