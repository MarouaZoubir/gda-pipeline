{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idhistory,
datedernieremodification,
intitule,
dateoperation,
description,
iduser
    FROM {{ source('gda_raw', 'history') }}
)

SELECT
    *
FROM source
