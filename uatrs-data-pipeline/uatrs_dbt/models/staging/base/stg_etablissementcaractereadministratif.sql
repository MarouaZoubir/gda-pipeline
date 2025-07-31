{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idetablissementcaractereadministratif,
datedernieremodification,
intitule,
abreviation,
ville,
ordre,
adresse,
email,
telephone,
fax,
idtypeetablissement
    FROM {{ source('gda_raw', 'etablissementcaractereadministratif') }}
)

SELECT
    *
FROM source
