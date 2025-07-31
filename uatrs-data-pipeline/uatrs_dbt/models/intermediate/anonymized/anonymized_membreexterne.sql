{{
  config(
    materialized='view',
    tags=['intermediate', 'pii_anonymized']
  )
}}

SELECT
  idMembreExterne,
  {{ hash_with_salt("email") }} AS hashed_email,
  {{ anonymize('cin', 'mask', 4) }} AS masked_cin,
  {{ anonymize('numeropasseport', 'mask', 4) }} AS masked_passport,
  CASE
    WHEN telephone IS NOT NULL THEN 
      CASE 
        WHEN telephone::text ~ '^\+212\d+$' THEN '+*** ' || RIGHT(telephone::text, 2)
        WHEN telephone::text ~ '^0\d+$' THEN '+*** ' || RIGHT(telephone::text, 2)
        ELSE '***-' || RIGHT(telephone::text, 4)
      END
    ELSE NULL
  END AS masked_phone,
  {{ anonymize('nom', 'mask', 1) }} AS nom_anon,
  {{ anonymize('prenom', 'mask', 1) }} AS prenom_anon,
  ville,
  datedernieremodification,
  idcivilite,
  idpays,
  idspecialite,
  orcid,
  isactive,
  iduser,
  datedebutaffectation,
  datefinaffectation,
  idetablissementcaractereadministratif,
  idetablissementcaracterecommercial,
  idetablissementnonuniversitaire,
  dtype,
  laboratoire AS idLaboratoire,
  -- Add fax anonymization here
  CASE
    WHEN fax IS NOT NULL THEN 
      CASE 
        WHEN fax::text ~ '^\+212\d+$' THEN '+*** ' || RIGHT(fax::text, 2)
        WHEN fax::text ~ '^0\d+$' THEN '+*** ' || RIGHT(fax::text, 2)
        ELSE '***-' || RIGHT(fax::text, 4)
      END
    ELSE NULL
  END AS masked_fax
FROM {{ ref('stg_membreexterne') }}