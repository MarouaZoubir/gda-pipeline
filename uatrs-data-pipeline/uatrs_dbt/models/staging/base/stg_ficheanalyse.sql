{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idficheanalyse,
datedernieremodification,
datedemande,
iddevis,
idlaboratoire,
caracteristiqueechantillon,
commentaire
    FROM {{ source('gda_raw', 'ficheanalyse') }}
)

SELECT
    *
FROM source
