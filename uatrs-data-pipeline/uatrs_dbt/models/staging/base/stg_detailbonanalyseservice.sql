{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailbonanalyseservice,
datedernieremodification,
idbonanalyse,
idservice
    FROM {{ source('gda_raw', 'detailbonanalyseservice') }}
)

SELECT
    *
FROM source
