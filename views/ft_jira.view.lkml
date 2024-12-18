view: ft_jira {
  sql_table_name: default_dw.ft_jira ;;
  suggestions: no

  dimension: asset {
    type: string
    sql: ${TABLE}.asset ;;
  }
  dimension: criticidade {
    type: string
    sql: ${TABLE}.criticidade ;;
  }
  dimension: data_de_criacao {
    type: string
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
  measure: count {
    type: count
  }
}
