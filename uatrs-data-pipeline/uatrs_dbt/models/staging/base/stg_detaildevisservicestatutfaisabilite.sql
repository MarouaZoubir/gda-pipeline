{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetaildevisservicestatutfaisabilite,
datedernieremodification,
commentaire,
iddevis,
idservice,
idstatutfaisabilitedemandedevis
    FROM {{ source('gda_raw', 'detaildevisservicestatutfaisabilite') }}
)

SELECT
    *
FROM source
