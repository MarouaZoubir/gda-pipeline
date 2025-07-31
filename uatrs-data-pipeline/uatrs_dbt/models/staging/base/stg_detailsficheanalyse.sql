{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailsficheanalyse,
datedernieremodification,
iddetailchampficheanalysevaleurproposeeficheanalyse,
iddetailsouschampficheanalysevaleurproposeeficheanalyse,
idficheanalyse,
valeur,
idbonanalyse,
iddevis,
idvaleurproposeeficheanalyse,
numerodemandeficheanalyse,
iddemandeficheanalyse,
isconsommee
    FROM {{ source('gda_raw', 'detailsficheanalyse') }}
)

SELECT
    *
FROM source
