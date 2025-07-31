{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetaillaboratoireunitemesure,
datedernieremodification,
idlaboratoire,
idunitemesure
    FROM {{ source('gda_raw', 'detaillaboratoireunitemesure') }}
)

SELECT
    *
FROM source
