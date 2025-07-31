{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idgrade,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'grade') }}
)

SELECT
    *
FROM source
