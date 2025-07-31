{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddiplome,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'diplome') }}
)

SELECT
    *
FROM source
