{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_typeanalyse AS (
    SELECT
        ta."idtypeanalyse",
        ta."prestation",
        ta."tariforganismeadministratif",
        ta."tariforganismecommercial",
        ta."intitule",
        ta."desactivation",
        ta."disponibilite",
        ta."idlaboratoire",
        lab."abreviation" AS laboratoire
    FROM 
        {{ ref('stg_typeanalyse') }} ta
    LEFT JOIN 
        {{ ref('stg_laboratoire') }} lab ON lab."idlaboratoire" = ta."idlaboratoire"
)
SELECT * FROM dim_typeanalyse
