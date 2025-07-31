{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idtarif,
datedernieremodification,
qtemax,
qtemin,
tariforganismeadministratif,
tariforganismecommercial,
idlaboratoire,
idtypeanalyse,
idtypetarif,
idunitemesure,
qteengroupe,
idqteamultiplierpar,
expiration
    FROM {{ source('gda_raw', 'tarif') }}
)

SELECT
    *
FROM source
