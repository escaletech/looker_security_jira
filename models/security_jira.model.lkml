connection: "serving_layer"

include: "/views/ft_jira.view.lkml"

label: "Dados - JIRA"

## EXPLORE DO JIRA

explore: ft_jira {
  label: "Dados Gerais"
  description: "Base principal com os dados de segurança abertos no Jira"
}
