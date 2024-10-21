{{
  config(
    materialized='table',
    unique_key='_id',
  )
}}

select 
    * 
from {{ source('sandbox', 'hubchat_itau') }}