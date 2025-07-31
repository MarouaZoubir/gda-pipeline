{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailresponsablelaboratoirelaboratoire,
datedernieremodification,
datedebutaffectation,
datefinaffectation,
isactive,
idlaboratoire,
idresponsablelaboratoire
    FROM {{ source('gda_raw', 'detailresponsablelaboratoirelaboratoire') }}
)

SELECT
    *
FROM source
