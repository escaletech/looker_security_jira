/* generating dates using the macro from the dbt-utils package */
with 
    dates_raw as (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2012-01-01' as date)",
            end_date="cast('2050-01-01' as date)"
        )
        }}
)

/* extracting some date information and renaming some columns to Portuguese */
, days_info as (
    select 
        date_format(date_day,'yyyyMMdd') data_id
        , cast(date_day as date) as data_dia
        , extract(dayofweek from date_day) as dia_da_semana
        , extract(month from date_day) as mes
        , extract(quarter from date_day) as trimestre
        , dayofyear(date_day) as dia_do_ano
        , extract(year from date_day) as ano
        , date_format(date_day,'yyyy-MM-dd') as mes_ingles
        , date_format(date_day, 'MM-dd') as dia_mes
    from dates_raw
)

/* renaming the column meanings, translating them into Portuguese */
    , days_named as (
        select
            *
            , case
                when dia_da_semana = 1 then 'Domingo'
                when dia_da_semana = 2 then 'Segunda-feira'
                when dia_da_semana = 3 then 'Terça-feira'
                when dia_da_semana = 4 then 'Quarta-feira'
                when dia_da_semana = 5 then 'Quinta-feira'
                when dia_da_semana = 6 then 'Sexta-feira'
                else 'Sábado' 
            end as nome_do_dia
            , case
                when mes = 1 then 'Janeiro'
                when mes = 2 then 'Fevereiro'
                when mes = 3 then 'Março'
                when mes = 4 then 'Abril'
                when mes = 5 then 'Maio'
                when mes = 6 then 'Junho'
                when mes = 7 then 'Julho'
                when mes = 8 then 'Agosto'
                when mes = 9 then 'Setembro'
                when mes = 10 then 'Outubro'
                when mes = 11 then 'Novembro'
                else 'Dezembro' 
            end as nome_do_mes
            , case
                when mes = 1 then 'Jan'
                when mes = 2 then 'Fev'
                when mes = 3 then 'Mar'
                when mes = 4 then 'Abr'
                when mes = 5 then 'Mai'
                when mes = 6 then 'Jun'
                when mes = 7 then 'Jul'
                when mes = 8 then 'Ago'
                when mes = 9 then 'Set'
                when mes = 10 then 'Out'
                when mes = 11 then 'Nov'
                else 'Dez' 
            end as abrev_do_mes
            , case
                when trimestre = 1 then '1º Trimestre'
                when trimestre = 2 then '2º Trimestre'
                when trimestre = 3 then '3º Trimestre'
                else '4º Trimestre' 
            end as nome_trimestre
            , case
                when trimestre in(1,2) then 1
                else 2
            end as semestre
            , case
                when trimestre in(1,2) then '1º Semestre'
                else '2º Semestre'
            end as nome_semestre
        from days_info
    )

/* rearranging the columns */
    , final_cte as (
        select 
            data_id
            , data_dia
            , date_format(data_dia,'yyyy-MM-01')  dt_mes
            , dia_da_semana
            , nome_do_dia
            , mes
            , mes_ingles
            , nome_do_mes
            , abrev_do_mes
            , trimestre
            , nome_trimestre
            , semestre
            , nome_semestre
            , dia_do_ano
            , ano
        from days_named
    )

select * 
from final_cte