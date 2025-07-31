{{
  config(
    materialized='incremental',
    schema='intermediate',
    tags=['intermediate', 'pii_anonymized'],
    unique_key='idmembreuatrs'
  )
}}

WITH source AS (
    SELECT
        idmembreuatrs,
        datedernieremodification,
        {{ anonymize('adresse', 'mask', 5) }} AS adresse_anon,  -- Shows last 5 chars
        {{ anonymize('cin', 'hash') }} AS cin_anon,
        {{ anonymize('nom', 'mask', 1) }} AS nom_anon,  -- Shows first letter only
        {{ anonymize('numeroimmatriculation', 'hash') }} AS immatriculation_anon,
        {{ anonymize('prenom', 'mask', 1) }} AS prenom_anon,
        {{ anonymize('telephone', 'mask', 2) }} AS telephone_anon,  -- Shows last 2 digits
        idcivilite,
        idgrade,
        iduser
    FROM {{ ref('stg_membreuatrs') }}
    {% if is_incremental() %}
    WHERE datedernieremodification > (SELECT MAX(datedernieremodification) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source