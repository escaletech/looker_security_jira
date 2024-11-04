with cte_vertical_data AS(
    SELECT -1 vertical_id, 'NAO INFORMADO' AS vertical
    UNION ALL
    SELECT 1, 'FINANCE'
    UNION ALL
    SELECT 2, 'HOMESERVICES'
)
select * from cte_vertical_data