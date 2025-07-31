{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailequipementtypeanalyse,
datedernieremodification,
idequipement,
idtypeanalyse
    FROM {{ source('gda_raw', 'detailequipementtypeanalyse') }}
)

SELECT
    *
FROM source
