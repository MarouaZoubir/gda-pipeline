{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchampficheanalysesouschampficheanalyse,
datedernieremodification,
idchampficheanalyse,
idsouschampficheanalyse
    FROM {{ source('gda_raw', 'detailchampficheanalysesouschampficheanalyse') }}
)

SELECT
    *
FROM source
