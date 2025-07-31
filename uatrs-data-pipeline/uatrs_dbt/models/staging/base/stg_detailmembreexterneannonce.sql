{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailmembreexterneannonce,
datedernieremodification,
idannonce,
idmembreexterne
    FROM {{ source('gda_raw', 'detailmembreexterneannonce') }}
)

SELECT
    *
FROM source
