{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idstatutconfirmationdemandebonanalyse,
datedernieremodification,
abreviation,
intitule,
ordre
    FROM {{ source('gda_raw', 'statutconfirmationdemandebonanalyse') }}
)

SELECT
    *
FROM source
