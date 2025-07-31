{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_etudiant AS (
    SELECT 
        etd."idetudiant" as etdId, 
        etd.masked_cin, 
        etd.masked_cne, 
        concat(etd.prenom_anon, ' ', etd.nom_anon) AS nom_complet, 
        etd."iddiplome", 
        dp.abreviation AS diplome, 
        eu."idetablissementuniversitaire", 
        eu.nom AS EtablissementUniversitaire
    FROM 
        {{ ref('anonymized_etudiant') }} etd
    LEFT JOIN 
        {{ ref('stg_diplome') }} dp ON dp."iddiplome" = etd."iddiplome"
    LEFT JOIN 
        {{ ref('stg_etablissementuniversitaire') }} eu ON eu."idetablissementuniversitaire" = etd."idetablissementuniversitaire"
)
SELECT * FROM dim_etudiant