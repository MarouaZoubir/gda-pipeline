{{ config(
    materialized='view',
    schema='staging',
    tags=['staging','raw']
) }}

WITH source AS (
    SELECT
        idbonanalyse,
datedernieremodification,
commentairevalidation,
datecreation,
datestatut,
isactuel,
montantinitial,
numeroba,
idetablissementconventionne,
idprofesseur,
idservice,
idstatutbonanalyse,
idstatutconfirmationdemandebonanalyse,
active,
suivieparuser
    FROM {{ source('gda_raw', 'bonanalyse') }}
)

SELECT
    *
FROM source
