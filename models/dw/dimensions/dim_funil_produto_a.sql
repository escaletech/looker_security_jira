with cte_select as(
    SELECT
        *
        , 'historico' as type_database
    FROM refined_finance_general.general_hubchat_fact
)
,cte_from as (
    SELECT
          *, 'historico' as type_database
      FROM refined_finance_general.general_hubchat_from
)
,cte_context (
    SELECT
          *, 'historico' as type_database
      FROM refined_finance_general.general_hubchat_context
      GROUP BY ALL
)

SELECT
    session_id,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' THEN general_hubchat_from.session_init ELSE NULL END `general_hubchat_from_total_first_interaction_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.opt_out= 'true' THEN 1 ELSE 0 END `general_hubchat_from_total_first_interaction_optout_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.opt_outIS NULL THEN 1 ELSE 0 END `general_hubchat_from_total_first_interaction_optin_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.unlock_inss IS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_unlock_inss_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.product_descriptionIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_simulation_geral_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.product_descriptionIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_description_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.simulation_productIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_simulation_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.show_productIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_show_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.loan_amount IS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_loan_amount_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.show_productIS NOT NULL AND general_hubchat_context.birth_dateIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_personal_data_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.show_productIS NOT NULL AND general_hubchat_context.birth_dateIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_birth_data_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.marital_statusIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_marital_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.genderIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_gender_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.place_of_birth IS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_place_of_birth_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.birth_city IS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_birth_city_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.national_identityIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_rg_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.issuer_national_identityIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_issuer_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.issue_state_national_identityIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_uf_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.issuer_national_identityIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_data_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_zip_codeIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_zipcode_geral_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_zip_codeIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_zipcode_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_streetIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_street_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_numberIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_number_address_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_neighborhoodIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_neighborhood_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_cityIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_city_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.address_stateIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_state_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.bankIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_bank_geral_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.bankIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_bank_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.bank_agencyIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_agency_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.bank_accountIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_bank_account_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.bank_confirmationIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_bank_confirmation_product_consig`,
    CASE WHEN general_hubchat_context.welcome = 'true' AND general_hubchat_context.token = 'C3Dk8S2EbKCmtdqB' AND general_hubchat_context.transfer_humanIS NOT NULL THEN 1 ELSE 0 END `general_hubchat_from_transfer_human_product_consig`

FROM cte_select AS general_hubchat_fact
LEFT JOIN cte_from AS general_hubchat_from ON general_hubchat_fact.session_init = general_hubchat_from.session_init AND general_hubchat_fact.token = general_hubchat_from.token
LEFT JOIN cte_context AS general_hubchat_context ON general_hubchat_fact.session_init = general_hubchat_context.session AND general_hubchat_fact.token = general_hubchat_context.token
GROUP BY
    (DATE_FORMAT(general_hubchat_from.timestamp_init , 'yyyy-MM-dd'))
ORDER BY
    1 DESC