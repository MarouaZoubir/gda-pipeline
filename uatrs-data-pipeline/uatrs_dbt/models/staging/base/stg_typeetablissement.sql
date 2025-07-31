{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idtypeetablissement,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'typeetablissement') }}
)

SELECT
    *
FROM source
