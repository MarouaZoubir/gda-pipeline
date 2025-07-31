{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        dtype,
idmembreexterne,
datedernieremodification,
adresse,
cin,
email,
fax,
nom,
numeropasseport,
prenom,
telephone,
orcid,
datedebutaffectation,
datefinaffectation,
isactive,
ville,
idcivilite,
idpays,
iduser,
idfaculteecole,
idetablissementnonuniversitaire,
equipe,
laboratoire,
idgrade,
idspecialite,
idetablissementcaractereadministratif,
idetablissementcaracterecommercial,
ice
    FROM {{ source('gda_raw', 'membreexterne') }}
)

SELECT
    *
FROM source
