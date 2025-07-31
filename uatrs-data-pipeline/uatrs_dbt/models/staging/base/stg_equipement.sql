{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idequipement,
datedernieremodification,
natureequipement,
ordre,
designation,
marque,
reference,
idstatutequipement
    FROM {{ source('gda_raw', 'equipement') }}
)

SELECT
    *
FROM source
