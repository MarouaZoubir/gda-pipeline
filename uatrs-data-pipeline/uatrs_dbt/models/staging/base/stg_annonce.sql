{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idannonce,
datedernieremodification,
datedebutannonce,
datefinannonce,
description,
enabled,
intitule
    FROM {{ source('gda_raw', 'annonce') }}
)

SELECT
    *
FROM source
