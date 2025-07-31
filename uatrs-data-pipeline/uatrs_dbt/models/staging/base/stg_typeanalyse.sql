{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idtypeanalyse,
datedernieremodification,
prestation,
tariforganismeadministratif,
tariforganismecommercial,
idlaboratoire,
intitule,
desactivation,
disponibilite
    FROM {{ source('gda_raw', 'typeanalyse') }}
)

SELECT
    *
FROM source
