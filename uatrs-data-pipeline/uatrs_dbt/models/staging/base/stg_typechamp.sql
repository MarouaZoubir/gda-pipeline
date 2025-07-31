{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idtypechamp,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'typechamp') }}
)

SELECT
    *
FROM source
