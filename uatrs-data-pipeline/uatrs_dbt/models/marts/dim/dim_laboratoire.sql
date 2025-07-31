{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_laboratoire AS (
    SELECT
        lab."idlaboratoire",
        lab."abreviation",
        lab."ordre",
        lab."tariforganismeadministratif",
        lab."tariforganismecommercial",
        lab."desactivation",
        lab."disponibilite",
        lab."idservice",
        s."abreviation" AS service,
        s."ordre" AS service_ordre,
        s."consommationpartielleba" AS consommationPartielleBA
    FROM 
        {{ ref('stg_laboratoire') }} lab
    LEFT JOIN 
        {{ ref('stg_service') }} s ON s."idservice" = lab."idservice"
)
SELECT * FROM dim_laboratoire
