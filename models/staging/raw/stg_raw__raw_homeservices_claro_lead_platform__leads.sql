with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_lead_platform__leads') }}

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
        company_category,
        company_cnpj,
        company_name,
        contacts_emails_other,
        contacts_emails_primary,
        contacts_emails_secondary,
        contacts_phones_other_ddd,
        contacts_phones_other_phone,
        contacts_phones_primary_ddd,
        contacts_phones_primary_phone,
        contacts_phones_secondary_ddd,
        contacts_phones_secondary_phone,
        created_at,
        created_by,
        crm_channels_hubspot_id,
        customer_has_cnpj,
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
