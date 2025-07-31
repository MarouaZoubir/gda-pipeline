{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_demandeficheanalyse AS (
    SELECT DISTINCT
        dfa."iddemandeficheanalyse",
        dfa."idbonanalyse",
        dfa."iddevis",
        sp.abreviation AS Specialite,
        dip.abreviation AS Diplome,
        sdfa.abreviation AS StatutDemandeFicheAnalyse,
        mp.abreviation AS MoyenPaiement
    FROM 
        {{ ref('anonymized_demandeficheanalyse') }} dfa
    LEFT JOIN 
        {{ ref('stg_specialite') }} sp ON sp."idspecialite" = dfa."idspecialite"
    LEFT JOIN 
        {{ ref('stg_statutdemandeficheanalyse') }} sdfa ON sdfa."idstatutdemandeficheanalyse" = dfa."idstatutdemandeficheanalyse"
    LEFT JOIN 
        {{ ref('stg_moyenpaiement') }} mp ON mp."idmoyenpaiement" = dfa."idmoyenpaiement"
    LEFT JOIN 
        {{ ref('stg_diplome') }} dip ON dip."iddiplome" = dfa."iddiplome"
)
SELECT * FROM dim_demandeficheanalyse
