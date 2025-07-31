{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddotation,
datedernieremodification,
dateattribution,
isactive,
modifiable,
montant,
idetablissementconventionne,
idfaculteecole
    FROM {{ source('gda_raw', 'dotation') }}
)

SELECT
    *
FROM source
