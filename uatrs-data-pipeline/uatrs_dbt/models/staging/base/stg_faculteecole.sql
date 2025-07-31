{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idfaculteecole,
datedernieremodification,
adresse,
nom,
signataires,
idetablissementuniversitaire
    FROM {{ source('gda_raw', 'faculteecole') }}
)

SELECT
    *
FROM source
