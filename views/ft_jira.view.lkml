view: ft_jira {
  sql_table_name: default_dw.ft_jira ;;
  suggestions: yes

  dimension: asset {
    type: string
    sql: ${TABLE}.asset ;;
  }

  dimension: criticidade {
    type: string
    sql: ${TABLE}.criticidade ;;
  }

  dimension_group: data_de_criacao {
    label: "Data de Criação"
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      week_of_year,
      day_of_week_index,
      month,
      month_name,
      day_of_month,
      day_of_week,
      quarter,
      year
    ]
    sql: ${TABLE}.data_de_criacao ;;
  }

  dimension: data_id {
    type: string
    sql: ${TABLE}.data_id ;;
  }

  dimension: issue_id {
    type: string
    sql: ${TABLE}.issue_id ;;
  }

  dimension: issue_key {
    type: string
    sql: ${TABLE}.issue_key ;;
  }

  dimension: jql_query {
    type: string
    sql: ${TABLE}.jql_query ;;
  }

  dimension: lead_time {
    type: string
    sql: ${TABLE}.lead_time ;;
  }

  dimension: origem_1 {
    type: string
    sql: ${TABLE}.origem_1 ;;
  }

  dimension: responsavel {
    type: string
    sql: ${TABLE}.responsavel ;;
  }

  dimension: resumo {
    type: string
    sql: ${TABLE}.resumo ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }
}
