{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idcorrespondance,
datedernieremodification,
dateenvoi,
messsage,
objet,
idtypecorrespondance,
iduser
    FROM {{ source('gda_raw', 'correspondance') }}
)

SELECT
    *
FROM source
