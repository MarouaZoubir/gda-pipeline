{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutoperationanalyse,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'statutoperationanalyse') }}
)

SELECT
    *
FROM source
