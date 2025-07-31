{{
  config(
    materialized='incremental',
    schema='intermediate',
    tags=['intermediate', 'pii_anonymized'],
    unique_key='idfaculteecole'
  )
}}

SELECT
    idfaculteecole,
    datedernieremodification,
    {{ anonymize('adresse', 'mask', 10) }} AS adresse_anon,
    nom,
    {{ anonymize_signatures('signataires') }} AS signataires_anon,
    idetablissementuniversitaire
FROM {{ ref('stg_faculteecole') }}
{% if is_incremental() %}
WHERE datedernieremodification > (SELECT MAX(datedernieremodification) FROM {{ this }})
{% endif %}