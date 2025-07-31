{{
  config(
    materialized='incremental',
    schema='intermediate',
    tags=['intermediate', 'pii_anonymized'],
    unique_key='idetablissementcaracterecommercial'
  )
}}

SELECT
    idetablissementcaracterecommercial,
    datedernieremodification,
    intitule,
    abreviation,
    domaine,
    ville,
    ordre,
    {{ anonymize('adresse', 'mask', 10) }} AS adresse_anon,
    {{ anonymize('email', 'mask', 3) }} AS email_anon,
    {{ anonymize('telephone', 'mask', 2) }} AS telephone_anon,
    {{ anonymize('fax', 'mask', 2) }} AS fax_anon,
    idsecteur
FROM {{ ref('stg_etablissementcaracterecommercial') }}
{% if is_incremental() %}
WHERE datedernieremodification > (SELECT MAX(datedernieremodification) FROM {{ this }})
{% endif %}