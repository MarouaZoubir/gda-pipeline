{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idtypetarif,
datedernieremodification,
abreviation,
intitule,
ordre,
qteengroupe
    FROM {{ source('gda_raw', 'typetarif') }}
)

SELECT
    *
FROM source
