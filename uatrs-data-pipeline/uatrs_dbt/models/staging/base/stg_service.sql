{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idservice,
datedernieremodification,
abreviation,
intitule,
ordre,
consommationpartielleba
    FROM {{ source('gda_raw', 'service') }}
)

SELECT
    *
FROM source
