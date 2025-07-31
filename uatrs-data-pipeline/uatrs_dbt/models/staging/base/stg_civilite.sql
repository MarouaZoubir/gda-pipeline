{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idcivilite,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'civilite') }}
)

SELECT
    *
FROM source
