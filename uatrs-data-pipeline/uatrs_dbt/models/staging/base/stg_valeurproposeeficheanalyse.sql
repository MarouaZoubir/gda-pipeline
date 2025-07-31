{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idvaleurproposeeficheanalyse,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'valeurproposeeficheanalyse') }}
)

SELECT
    *
FROM source
