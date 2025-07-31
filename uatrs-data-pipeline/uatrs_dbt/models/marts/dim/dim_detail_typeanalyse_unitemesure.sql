{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH detail_type_analyse_unite_mesure AS (
    SELECT
        dtu."iddetailtypeanalyseunitemesure",
        dtu."idtypeanalyse",
        ta."prestation",
        dtu."idunitemesure",
        um."abreviation"
    FROM 
        {{ ref('stg_detailtypeanalyseunitemesure') }} dtu
    LEFT JOIN 
        {{ ref('stg_typeanalyse') }} ta ON ta."idtypeanalyse" = dtu."idtypeanalyse"
    LEFT JOIN 
        {{ ref('stg_unitemesure') }} um ON um."idunitemesure" = dtu."idunitemesure"
)
SELECT * FROM detail_type_analyse_unite_mesure
