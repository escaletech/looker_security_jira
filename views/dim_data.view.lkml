view: dim_data {
  sql_table_name: default_dw.dim_data ;;
  suggestions: no

  dimension: abrev_do_mes {
    type: string
    sql: ${TABLE}.abrev_do_mes ;;
  }
  dimension: ano {
    type: number
    sql: ${TABLE}.ano ;;
  }
  dimension_group: data_dia {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.data_dia ;;
  }
  dimension: data_id {
    type: string
    sql: ${TABLE}.data_id ;;
  }
  dimension: dia_da_semana {
    type: number
    sql: ${TABLE}.dia_da_semana ;;
  }
  dimension: dia_do_ano {
    type: number
    sql: ${TABLE}.dia_do_ano ;;
  }
  dimension: dt_mes {
    type: string
    sql: ${TABLE}.dt_mes ;;
  }
  dimension: mes {
    type: number
    sql: ${TABLE}.mes ;;
  }
  dimension: mes_ingles {
    type: string
    sql: ${TABLE}.mes_ingles ;;
  }
  dimension: nome_do_dia {
    type: string
    sql: ${TABLE}.nome_do_dia ;;
  }
  dimension: nome_do_mes {
    type: string
    sql: ${TABLE}.nome_do_mes ;;
  }
  dimension: nome_semestre {
    type: string
    sql: ${TABLE}.nome_semestre ;;
  }
  dimension: nome_trimestre {
    type: string
    sql: ${TABLE}.nome_trimestre ;;
  }
  dimension: semestre {
    type: number
    sql: ${TABLE}.semestre ;;
  }
  dimension: trimestre {
    type: number
    sql: ${TABLE}.trimestre ;;
  }
  measure: count {
    type: count
  }
}
