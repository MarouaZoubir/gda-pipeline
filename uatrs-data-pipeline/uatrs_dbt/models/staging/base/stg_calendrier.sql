{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idcalendrier,
datedernieremodification
    FROM {{ source('gda_raw', 'calendrier') }}
)

SELECT
    *
FROM source
