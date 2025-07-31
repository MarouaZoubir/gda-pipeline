{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutconfirmationdemandedevis,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'statutconfirmationdemandedevis') }}
)

SELECT
    *
FROM source
