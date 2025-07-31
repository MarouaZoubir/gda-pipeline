{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetaildevisservice,
datedernieremodification,
iddevis,
idservice
    FROM {{ source('gda_raw', 'detaildevisservice') }}
)

SELECT
    *
FROM source
