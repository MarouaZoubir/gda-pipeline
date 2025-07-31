{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchargedemandeanalysedevis,
datedernieremodification,
idchargedemandeanalyse,
iddevis
    FROM {{ source('gda_raw', 'detailchargedemandeanalysedevis') }}
)

SELECT
    *
FROM source
