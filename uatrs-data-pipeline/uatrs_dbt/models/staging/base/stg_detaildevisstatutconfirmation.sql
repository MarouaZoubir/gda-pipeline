{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetaildevisstatutconfirmation,
datedernieremodification,
iddevis,
idstatutconfirmationdemandedevis,
commentaire
    FROM {{ source('gda_raw', 'detaildevisstatutconfirmation') }}
)

SELECT
    *
FROM source
