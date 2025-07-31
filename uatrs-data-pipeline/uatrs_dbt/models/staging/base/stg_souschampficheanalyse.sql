{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idsouschampficheanalyse,
datedernieremodification,
abreviation,
intitule,
ordre,
idtypeanalyse
    FROM {{ source('gda_raw', 'souschampficheanalyse') }}
)

SELECT
    *
FROM source
