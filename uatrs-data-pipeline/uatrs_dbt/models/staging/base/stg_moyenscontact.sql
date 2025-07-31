{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idmoyenscontact,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'moyenscontact') }}
)

SELECT
    *
FROM source
