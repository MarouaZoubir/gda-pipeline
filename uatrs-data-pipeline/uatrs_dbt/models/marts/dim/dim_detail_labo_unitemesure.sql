{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH detail_laboratoire_unite_mesure AS (
    SELECT
        dlu."iddetaillaboratoireunitemesure",
        dlu."idlaboratoire",
        lab."abreviation" AS laboratoire,
        dlu."idunitemesure",
        um."abreviation" AS unitemesure
    FROM 
        {{ ref('stg_detaillaboratoireunitemesure') }} dlu
    LEFT JOIN 
        {{ ref('stg_laboratoire') }} lab ON lab."idlaboratoire" = dlu."idlaboratoire"
    LEFT JOIN 
        {{ ref('stg_unitemesure') }} um ON um."idunitemesure" = dlu."idunitemesure"
)
SELECT * FROM detail_laboratoire_unite_mesure