{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idlaboratoire,
datedernieremodification,
abreviation,
intitule,
ordre,
idservice,
tariforganismeadministratif,
tariforganismecommercial,
desactivation,
disponibilite
    FROM {{ source('gda_raw', 'laboratoire') }}
)

SELECT
    *
FROM source
