with cte_vertical_data AS(
    select -2 vertical_id, 'TESTE' AS vertical
    union all
    SELECT -1 , 'NAO INFORMADO'
    UNION ALL
    SELECT 1, 'FINANCE'
    UNION ALL
    SELECT 2, 'HOMESERVICES'

)
select * from cte_vertical_data