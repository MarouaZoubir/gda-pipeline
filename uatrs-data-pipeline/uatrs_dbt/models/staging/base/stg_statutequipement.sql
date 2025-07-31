{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutequipement,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'statutequipement') }}
)

SELECT
    *
FROM source
