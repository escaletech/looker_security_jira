with 

source as (

    select * from {{ source('trusted_finance_general', 'hubchat_escale_finance_messages_context') }}

),

renamed as (

    select
        _id,
        account_digit,
        additional_details,
        address_city,
        address_complement,
        address_neighborhood,
        address_number,
        address_state,
        address_street,
        address_zip_code,
        admission_date,
        amount_not_understand,
        amount_recoveries,
        amount_returns,
        ano_veiculo,
        api_assertiva_new,
        assertiva,
        bank,
        bank_account,
        bank_agency,
        bank_confirmation,
        birth_city,
        birth_date,
        body_api_score1,
        body_api_score2,
        body_api_score3,
        brand,
        business_name,
        car_situation,
        city_cnpj,
        city_of_origin,
        confirma_placa_veiculo,
        contract,
        data_confirmation,
        deal_origination,
        dealstage_label,
        desired_value,
        down_payment,
        down_payment_type,
        eligible_product,
        email,
        employer_type,
        employment_time,
        federal_tax_identification_number,
        finan_ano_modelo_do_veiculo,
        finan_etapa_da_qualificacao_wpp,
        finan_marca_veiculo,
        finan_modelo_veiculo,
        financial_issues,
        gender,
        history_hsm,
        how_many_days,
        id_contact_hubspot,
        id_deal_hubspot,
        income_value,
        individual_registration,
        installments,
        insurance_abc_test,
        insurance_abc_test_route,
        issue_date_national_identity,
        issue_state_national_identity,
        issuer_national_identity,
        km_veiculo,
        lake_created_at,
        lead_id_principal,
        life_cycle,
        marital_status,
        matriz_de_decisao,
        mother_name,
        name,
        national_identity,
        nationality,
        neighborhood_cnpj,
        not_understand,
        number_cnpj,
        numero_telefone,
        occupation,
        occupation_description,
        occupation_type,
        operation_brand,
        operation_product,
        operation_vertical,
        opt_in,
        opt_out,
        ownership,
        pension_number,
        placa_veiculo,
        place_of_birth,
        pre_simulate,
        preference_b2_c,
        presented_b2_c,
        product,
        product_brand,
        product_confirmation,
        product_deal_stage,
        product_deal_stage_description,
        product_description,
        product_km,
        product_model,
        product_state_of_origin,
        product_value,
        product_year,
        profession,
        proposal,
        proposal_id,
        purchase_situation,
        purchase_situation_description,
        purchase_time,
        recover,
        retorno_api_placas_address_city,
        retorno_api_placas_chassis_number,
        retorno_api_placas_status,
        retorno_api_placas_vehicle_type,
        retorno_pre_analise_is_eligible,
        retorno_score1,
        retorno_score2,
        retorno_score3,
        return,
        schooling,
        score1,
        score2,
        score3,
        second_phone_number,
        select_product,
        send_data,
        session,
        show_product,
        simulation_eligibility,
        simulation_product,
        sinister,
        start_checkout,
        state_cnpj,
        state_of_origin,
        street_cnpj,
        template_name_whatsapp,
        terms_conditions,
        timeout,
        timestamp,
        timestamp_disparo_hsm,
        timestamp_init,
        timestamp_recebimento_flow,
        timestamp_resposta_flow,
        tipo_compra,
        token,
        transfer_human,
        type_contact,
        type_product,
        type_product_description,
        type_seller,
        type_seller_description,
        uf_licenciamento,
        unlock_inss,
        update_version_date_time,
        valor_venda_veiculo,
        vehicle_model,
        vehicle_pcd,
        version,
        vertical,
        visited_b2_c,
        welcome,
        whatsapp_template_name,
        zipcode_cnpj,
        year,
        month,
        day,
        lake_key,
        element_name_hsm,
        insurance_answer,
        operation_source,
        loan_amount,
        amount_recoveries_intelligent,
        array_parcelas,
        asset_value,
        autorizacao_contato_especialista,
        avaliacao_abordagem,
        avaliacao_atendimento,
        avaliacao_perguntas_repetidas,
        avaliacao_preocupacao_necessidades,
        avaliacao_tempo_resposta,
        card_confirmation,
        card_number,
        card_printed_name,
        card_security_code,
        card_validity,
        codigo_simulacao,
        codigo_veiculo_molicar,
        contract_date_time,
        contract_id,
        customer,
        dados_contato_email,
        dados_contato_telefone,
        driver_license,
        duvidas_foram_sanadas,
        endereco_empresa_cliente,
        endereco_residencia_cliente,
        flow_token,
        issue_date_driver_license,
        issue_date_individual_registration,
        issue_state_driver_license,
        issue_state_individual_registration,
        issuer_driver_license,
        issuer_individual_registration,
        know_product,
        nickname,
        opcao_modelo,
        opcoes_carros,
        opiniao_interacao,
        order,
        order_date_time,
        order_id,
        parcelas_disponiveis,
        payday,
        payment_type,
        payment_type_description,
        percentual_min_entrada,
        phone_number,
        phone_number_holder,
        proposal_date_time,
        recover_timeout_intelligent,
        return_intelligent,
        second_phone_number_holder,
        simulation_flow_ending,
        third_phone_number,
        third_phone_number_holder,
        tipo_entrada,
        valor_min_entrada,
        valor_veiculo_molicar,
        vehicle_refinancing,
        webchat,
        csat_wpp_flow_response,
        lead_id,
        api_assertiva,
        payment_due_date,
        viability_product,
        viability_product_description,
        CHARINDEX('{', _id) AS first_position,
        rank() over(partition by session order by timestamp desc) as last_session

    from source
    QUALIFY last_session = 1
)

select 
    session message_session_id
    ,timestamp
    ,regexp_replace(numero_telefone, '[^0-9]', '') AS client_id
    ,token
    ,welcome
    ,opt_out
    ,unlock_inss
    ,product_description
    ,simulation_product
    ,show_product
    ,loan_amount
    ,birth_date
    ,marital_status
    ,gender
    ,place_of_birth
    ,birth_city
    ,national_identity
    ,issuer_national_identity
    ,issue_state_national_identity
    ,address_zip_code
    ,address_street
    ,address_number
    ,address_neighborhood
    ,address_city
    ,address_state
    ,bank
    ,bank_agency
    ,bank_account
    ,bank_confirmation
    ,transfer_human
    ,individual_registration
    ,product_deal_stage_description
    ,type_seller_description
    ,eligible_product
    ,product_confirmation
    ,down_payment_type
    ,income_value
    ,type_contact
    ,name
    ,email
    ,mother_name
    ,nationality
    ,profession
    ,type_product_description
    ,product_brand
    ,product_year
    ,product_model
    ,product_km
    ,product_value
    ,product_state_of_origin
    ,down_payment
    ,pre_simulate
    ,simulation_eligibility
    ,city_of_origin
    ,state_of_origin
    ,issue_date_national_identity
    ,employer_type
    ,pension_number
    ,federal_tax_identification_number
    ,zipcode_cnpj
    ,employment_time
    ,purchase_time
    ,purchase_situation_description
    ,occupation
    ,number_cnpj
from renamed
where true
    and first_position = 0 