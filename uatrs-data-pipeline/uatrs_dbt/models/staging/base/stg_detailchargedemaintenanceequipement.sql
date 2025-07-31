{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailchargedemaintenanceequipement,
datedernieremodification,
datedebutaffectation,
datefinaffectation,
isactive,
idchargedemaintenance,
idequipement
    FROM {{ source('gda_raw', 'detailchargedemaintenanceequipement') }}
)

SELECT
    *
FROM source
