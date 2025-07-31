{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailbonanalyseservicestatutfaisabilite,
datedernieremodification,
commentaire,
idbonanalyse,
idservice,
idstatutfaisabilitedemandebonanalyse
    FROM {{ source('gda_raw', 'detailbonanalyseservicestatutfaisabilite') }}
)

SELECT
    *
FROM source
