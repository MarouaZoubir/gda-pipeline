{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailbonanalysemoyenscontact,
datedernieremodification,
idbonanalyse,
idmoyenscontact
    FROM {{ source('gda_raw', 'detailbonanalysemoyenscontact') }}
)

SELECT
    *
FROM source
