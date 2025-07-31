{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idetablissementuniversitaire,
datedernieremodification,
adresse,
nom
    FROM {{ source('gda_raw', 'etablissementuniversitaire') }}
)

SELECT
    *
FROM source
