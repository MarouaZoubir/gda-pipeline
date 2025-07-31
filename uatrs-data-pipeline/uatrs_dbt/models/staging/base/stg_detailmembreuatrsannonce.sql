{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailmembreuatrsannonce,
datedernieremodification,
idannonce,
idmembreuatrs
    FROM {{ source('gda_raw', 'detailmembreuatrsannonce') }}
)

SELECT
    *
FROM source
