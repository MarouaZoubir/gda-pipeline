{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutfaisabilitedemandedevis,
datedernieremodification,
abreviation,
ordre
    FROM {{ source('gda_raw', 'statutfaisabilitedemandedevis') }}
)

SELECT
    *
FROM source
