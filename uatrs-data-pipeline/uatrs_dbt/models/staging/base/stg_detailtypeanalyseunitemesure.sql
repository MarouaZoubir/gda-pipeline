{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailtypeanalyseunitemesure,
datedernieremodification,
idtypeanalyse,
idunitemesure
    FROM {{ source('gda_raw', 'detailtypeanalyseunitemesure') }}
)

SELECT
    *
FROM source
