{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddossieranalyse,
datedernieremodification,
datecreation,
idfacturation,
idbonanalyse,
iddevis,
idetudiant,
idlaboratoire,
numerodemandeficheanalyse,
numerodossieranalyse,
suivieparuser
    FROM {{ source('gda_raw', 'dossieranalyse') }}
)

SELECT
    *
FROM source