{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailsdevis,
datedernieremodification,
iddevis,
iddetaillaboratoireunitemesure,
iddetailtypeanalyseunitemesure,
valeur
    FROM {{ source('gda_raw', 'detailsdevis') }}
)

SELECT
    *
FROM source
