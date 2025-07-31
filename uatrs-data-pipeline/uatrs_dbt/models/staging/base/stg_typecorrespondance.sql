{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idtypecorrespondance,
datedernieremodification,
intitule,
ordre
    FROM {{ source('gda_raw', 'typecorrespondance') }}
)

SELECT
    *
FROM source
