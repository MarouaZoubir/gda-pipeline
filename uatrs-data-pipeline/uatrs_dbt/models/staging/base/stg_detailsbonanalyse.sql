{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailsbonanalyse,
datedernieremodification,
idbonanalyse,
iddetaillaboratoireunitemesure,
iddetailtypeanalyseunitemesure,
valeur
    FROM {{ source('gda_raw', 'detailsbonanalyse') }}
)

SELECT
    *
FROM source
