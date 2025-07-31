{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idoperationanalyse,
datedernieremodification,
cheminrapportfile,
idficheanalyse,
idstatutoperationanalyse
    FROM {{ source('gda_raw', 'operationanalyse') }}
)

SELECT
    *
FROM source
