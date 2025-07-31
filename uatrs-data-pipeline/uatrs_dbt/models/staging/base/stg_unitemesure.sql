{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idunitemesure,
datedernieremodification,
abreviation,
intitule,
ordre,
valeur
    FROM {{ source('gda_raw', 'unitemesure') }}
)

SELECT
    *
FROM source
