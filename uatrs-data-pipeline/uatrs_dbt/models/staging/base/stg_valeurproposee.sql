{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idvaleurproposee,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'valeurproposee') }}
)

SELECT
    *
FROM source
