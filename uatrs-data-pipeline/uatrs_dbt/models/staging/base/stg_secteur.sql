{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idsecteur,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'secteur') }}
)

SELECT
    *
FROM source
