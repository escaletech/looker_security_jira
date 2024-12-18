select 
    * except(lake_created_at,year,month,day,lake_key)
from {{ ref('stg_trusted_homeservices_general__google_spreadsheet_de_para_digital_campaign') }}