{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutbonanalyse,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'statutbonanalyse') }}
)

SELECT
    *
FROM source
