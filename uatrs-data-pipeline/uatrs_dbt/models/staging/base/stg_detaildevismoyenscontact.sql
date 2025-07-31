{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetaildevismoyenscontact,
datedernieremodification,
iddevis,
idmoyenscontact
    FROM {{ source('gda_raw', 'detaildevismoyenscontact') }}
)

SELECT
    *
FROM source
