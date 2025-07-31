{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idordreanalyse,
datedernieremodification,
datecreation,
iddossieranalyse,
numeroordreanalyse
    FROM {{ source('gda_raw', 'ordreanalyse') }}
)

SELECT
    *
FROM source
