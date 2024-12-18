with cte_marca_data AS(
    SELECT -1 marca_id, 'NAO INFORMADO' AS marca
    UNION ALL
    SELECT 1, 'BV'
    UNION ALL
    SELECT 2, 'CLARO'
    UNION ALL
    SELECT 3, 'ITAU'
    UNION ALL
    SELECT 4, 'OI'
    UNION ALL
    SELECT 5, 'QISTA'
    UNION ALL
    SELECT 6, 'REENERGISA'
    UNION ALL
    SELECT 7, 'RODOBENS'
)
select * from cte_marca_data