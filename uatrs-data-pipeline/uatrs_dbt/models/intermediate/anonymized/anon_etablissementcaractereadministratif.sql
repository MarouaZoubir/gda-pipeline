{{
  config(
    materialized='incremental',
    schema='intermediate',
    tags=['intermediate', 'pii_anonymized'],
    unique_key='idetablissementcaractereadministratif'
  )
}}

SELECT
    idetablissementcaractereadministratif,
    datedernieremodification,
    intitule,
    abreviation,
    ville,
    ordre,
    {{ anonymize('adresse', 'mask', 10) }} AS adresse_anon,
    {{ anonymize('email', 'mask', 3) }} AS email_anon,  -- Shows first 3 chars of username
    {{ anonymize('telephone', 'mask', 2) }} AS telephone_anon,
    {{ anonymize('fax', 'mask', 2) }} AS fax_anon,
    idtypeetablissement
FROM {{ ref('stg_etablissementcaractereadministratif') }}
{% if is_incremental() %}
WHERE datedernieremodification > (SELECT MAX(datedernieremodification) FROM {{ this }})
{% endif %}