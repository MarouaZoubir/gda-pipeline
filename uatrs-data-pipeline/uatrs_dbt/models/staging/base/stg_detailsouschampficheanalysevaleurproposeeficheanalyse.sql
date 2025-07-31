{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailsouschampficheanalysevaleurproposeeficheanalyse,
datedernieremodification,
idsouschampficheanalyse,
idvaleurproposeeficheanalyse,
idtypechamp,
uniteverificationconsommation
    FROM {{ source('gda_raw', 'detailsouschampficheanalysevaleurproposeeficheanalyse') }}
)

SELECT
    *
FROM source
