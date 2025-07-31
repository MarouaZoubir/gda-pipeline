{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        dtype,
idfacturation,
datedernieremodification,
datecreation,
numero
    FROM {{ source('gda_raw', 'facturation') }}
)

SELECT
    *
FROM source
