{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailbonanalysestatutconfirmation,
datedernieremodification,
idbonanalyse,
idstatutconfirmationdemandebonanalyse,
commentaire
    FROM {{ source('gda_raw', 'detailbonanalysestatutconfirmation') }}
)

SELECT
    *
FROM source
