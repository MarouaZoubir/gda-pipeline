{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchampficheanalysevaleurproposeeficheanalyse,
datedernieremodification,
idchampficheanalyse,
idvaleurproposeeficheanalyse,
idtypechamp,
idficheanalyse,
uniteverificationconsommation
    FROM {{ source('gda_raw', 'detailchampficheanalysevaleurproposeeficheanalyse') }}
)

SELECT
    *
FROM source
