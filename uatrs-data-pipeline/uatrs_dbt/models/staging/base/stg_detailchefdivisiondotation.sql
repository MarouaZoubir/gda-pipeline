{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchefdivisiondotation,
datedernieremodification,
idchefdivision,
iddotation
    FROM {{ source('gda_raw', 'detailchefdivisiondotation') }}
)

SELECT
    *
FROM source
