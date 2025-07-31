{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idetudiant,
datedernieremodification,
cin,
cne,
nom,
numeropasseport,
prenom,
iddiplome,
idetablissementuniversitaire
    FROM {{ source('gda_raw', 'etudiant') }}
)

SELECT
    *
FROM source
