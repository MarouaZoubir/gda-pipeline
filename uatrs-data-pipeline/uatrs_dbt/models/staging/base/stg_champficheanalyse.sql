{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idchampficheanalyse,
datedernieremodification,
abreviation,
intitule,
ordre,
idlaboratoire
    FROM {{ source('gda_raw', 'champficheanalyse') }}
)

SELECT
    *
FROM source
