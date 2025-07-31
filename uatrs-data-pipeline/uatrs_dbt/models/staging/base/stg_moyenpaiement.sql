{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idmoyenpaiement,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'moyenpaiement') }}
)

SELECT
    *
FROM source
