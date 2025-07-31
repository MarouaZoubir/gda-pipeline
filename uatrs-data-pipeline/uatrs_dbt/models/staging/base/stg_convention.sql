{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idconvention,
datedernieremodification,
intitule,
datedebut,
datefin,
idetablissementcaractereadministratif,
idetablissementcaracterecommercial
    FROM {{ source('gda_raw', 'convention') }}
)

SELECT
    *
FROM source
