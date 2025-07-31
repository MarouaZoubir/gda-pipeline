{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutcorrespondance,
datedernieremodification,
intitule
    FROM {{ source('gda_raw', 'statutcorrespondance') }}
)

SELECT
    *
FROM source
