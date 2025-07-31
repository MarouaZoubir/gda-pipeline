{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchargedemandeanalysebonanalyse,
datedernieremodification,
idbonanalyse,
idchargedemandeanalyse
    FROM {{ source('gda_raw', 'detailchargedemandeanalysebonanalyse') }}
)

SELECT
    *
FROM source
