{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idetablissementcaracterecommercial,
datedernieremodification,
intitule,
abreviation,
domaine,
ville,
ordre,
adresse,
email,
telephone,
fax,
idsecteur
    FROM {{ source('gda_raw', 'etablissementcaracterecommercial') }}
)

SELECT
    *
FROM source
