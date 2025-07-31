{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        iddetailstatutcorrespondance,
datedernieremodification,
commentaire,
datestatut,
idcorrespondance,
idstatutcorrespondance,
iduser
    FROM {{ source('gda_raw', 'detailstatutcorrespondance') }}
)

SELECT
    *
FROM source
