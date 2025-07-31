{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idspecialite,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'specialite') }}
)

SELECT
    *
FROM source
