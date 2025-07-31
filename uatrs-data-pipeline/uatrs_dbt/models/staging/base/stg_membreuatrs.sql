{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        dtype,
idmembreuatrs,
datedernieremodification,
adresse,
cin,
nom,
numeroimmatriculation,
prenom,
telephone,
idcivilite,
idgrade,
iduser
    FROM {{ source('gda_raw', 'membreuatrs') }}
)

SELECT
    *
FROM source
