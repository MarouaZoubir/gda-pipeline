{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idauthoritie,
datedernieremodification,
authority,
iduser
    FROM {{ source('gda_raw', 'user_authorities') }}
)

SELECT
    *
FROM source
