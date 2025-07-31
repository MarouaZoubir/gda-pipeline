{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchefserviceservice,
datedernieremodification,
datedebutaffectation,
datefinaffectation,
isactive,
idchefservice,
idservice
    FROM {{ source('gda_raw', 'detailchefserviceservice') }}
)

SELECT
    *
FROM source
