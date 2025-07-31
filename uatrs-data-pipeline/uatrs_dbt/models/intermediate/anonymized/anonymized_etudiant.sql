{{
  config(
    materialized='view',
    tags=['intermediate', 'pii_anonymized']
  )
}}

WITH staged_data AS (
    SELECT 
        e.*,
        d.intitule AS diplome_libelle
    FROM {{ ref('stg_etudiant') }} e
    LEFT JOIN {{ ref('stg_diplome') }} d 
        ON e.iddiplome = d.iddiplome
)

SELECT
  idetudiant,
  {{ anonymize('cne', 'mask', 4) }} AS masked_cne,
  {{ anonymize('cin', 'mask', 4) }} AS masked_cin,
  {{ anonymize('nom', 'mask', 1) }} AS nom_anon,
  {{ anonymize('prenom', 'mask', 1) }} AS prenom_anon,
  iddiplome,
  diplome_libelle AS diploma_type,
  idetablissementuniversitaire
FROM staged_data