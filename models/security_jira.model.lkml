connection: "serving_layer"

include: "/views/ft_jira.view.lkml"
include: "/views/dim_data.view.lkml"


label: "Dados Segurança - Jira"


#############################################################################################
####################################   JIRA       ####################################
#############################################################################################


explore: ft_jira {
  label: "Dados gerais - Jira"
  view_label: "Jira - Segurança"
  description: "Base de dados que contém abertura de chamados e incidentes para o time de segurança"

  join: dim_data {
    view_label: "Data"
    relationship: many_to_one
    sql_on: ${ft_jira.data_id} = ${dim_data.data_id} ;;
  }
  }
