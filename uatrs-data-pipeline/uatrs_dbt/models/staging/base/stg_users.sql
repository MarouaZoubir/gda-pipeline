{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iduser,
datedernieremodification,
enabled,
initialized,
ismentionlegalvalid,
istested,
password,
username,
iscompteactive,
cleactivation
    FROM {{ source('gda_raw', 'users') }}
)

SELECT
    *
FROM source
