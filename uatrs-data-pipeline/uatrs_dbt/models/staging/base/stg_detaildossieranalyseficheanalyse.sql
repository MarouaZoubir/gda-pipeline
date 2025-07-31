{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        datedernieremodification,
iddossieranalyse,
idficheanalyse
    FROM {{ source('gda_raw', 'detaildossieranalyseficheanalyse') }}
)

SELECT
    *
FROM source
