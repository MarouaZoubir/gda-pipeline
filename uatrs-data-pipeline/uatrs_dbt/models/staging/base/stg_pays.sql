{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idpays,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'pays') }}
)

SELECT
    *
FROM source
