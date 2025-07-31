{{
  config(
    materialized='table',
    schema='analytics',
    tags=['dimension']
  )
}}

WITH dim_etablissementuniversitaire AS (
    SELECT
        eu."idetablissementuniversitaire" AS etablissement_universitaire_key,
        eu."nom" AS nom_etablissement,
        eu."adresse" AS adresse,
        -- Add any other relevant fields from the base table
        CURRENT_TIMESTAMP AS dbt_loaded_at
    FROM {{ ref('stg_etablissementuniversitaire') }} eu
)

SELECT * FROM dim_etablissementuniversitaire