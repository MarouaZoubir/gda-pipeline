{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddevis,
datedernieremodification,
datecreation,
montantinitial,
numerodevis,
idstatutconfirmationdemandedevis,
idparticulier,
consomme,
iddemandeuretablissementcaractereadministratif,
iddemandeuretablissementcaracterecommercial,
active
    FROM {{ source('gda_raw', 'devis') }}
)

SELECT
    *
FROM source
