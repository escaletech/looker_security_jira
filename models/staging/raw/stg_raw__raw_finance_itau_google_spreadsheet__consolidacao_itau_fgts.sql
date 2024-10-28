with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__consolidacao_itau_fgts') }}

),

renamed as (

    select
        agencia,
        bairro,
        banco_destinatario_credito,
        cidade_cliente,
        codigo_entidade,
        codigo_loja,
        codigo_tabela_fator,
        conta_corrente,
        cpf_cliente,
        cpf_digitador,
        cpf_vendedor,
        data_cancelamento,
        data_de_nascimento,
        data_digitacao,
        ddd,
        digito_conta_corrente,
        endereco,
        end_complemento,
        motivo_de_devolucao,
        nome_cliente,
        nome_entidade,
        nome_loja,
        nome_tabela_fator,
        numero_contrato,
        numero_operacao_ade,
        numero_residencia,
        quantidade_parcelas_contrato,
        situacao_proposta,
        telefone,
        uf_cliente,
        ultima_alteracao_de_status,
        usuario_inclusao,
        valor_bruto_solicitado,
        valor_financiado,
        valor_liberado,
        valor_total_emprestimo,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
