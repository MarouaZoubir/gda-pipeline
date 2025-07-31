{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailfaculteecoleprofesseur,
datedernieremodification,
idfaculteecole,
idprofesseur
    FROM {{ source('gda_raw', 'detailfaculteecoleprofesseur') }}
)

SELECT
    *
FROM source
