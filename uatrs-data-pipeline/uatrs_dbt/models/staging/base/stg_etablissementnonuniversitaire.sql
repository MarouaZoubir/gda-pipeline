{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        dtype,
idetablissementnonuniversitaire,
datedernieremodification,
adresse,
domaine,
nom,
dateconvention,
referenceconvention,
idsecteur,
etablissementtype
    FROM {{ source('gda_raw', 'etablissementnonuniversitaire') }}
)

SELECT
    *
FROM source
