{{
  config(
    materialized='table',
    schema='analytics',
    unique_key='tarif_key',
    tags=['dimension']
  )
}}

WITH 

-- Jointure des données tarifaires avec leurs métadonnées
tarif_data AS (
  SELECT
    t.idtarif,
    t.datedernieremodification,
    t.qtemax,
    t.qtemin,
    t.tariforganismeadministratif,
    t.tariforganismecommercial,
    t.idlaboratoire,
    t.idtypeanalyse,
    t.idtypetarif,
    t.idunitemesure,
    t.qteengroupe,
    t.idqteamultiplierpar,
    t.expiration,
    tt.abreviation AS type_tarif_code,
    tt.intitule AS type_tarif_libelle,
    tt.ordre AS type_tarif_ordre,
    tt.qteengroupe AS type_tarif_qte_groupe,
    ta.prestation AS type_analyse,
    ta.intitule AS description_analyse,
    um.abreviation AS unite_mesure_code,
    um.intitule AS unite_mesure_libelle,
    um.ordre AS unite_mesure_ordre,
    um.valeur AS unite_mesure_valeur,
    lab.abreviation AS laboratoire_code,
    lab.intitule AS laboratoire_libelle
  FROM {{ ref('stg_tarif') }} t
  LEFT JOIN {{ ref('stg_typetarif') }} tt ON t.idtypetarif = tt.idtypetarif
  LEFT JOIN {{ ref('stg_typeanalyse') }} ta ON t.idtypeanalyse = ta.idtypeanalyse
  LEFT JOIN {{ ref('stg_unitemesure') }} um ON t.idunitemesure = um.idunitemesure
  LEFT JOIN {{ ref('stg_laboratoire') }} lab ON t.idlaboratoire = lab.idlaboratoire
),

-- Calcul des indicateurs dérivés
tarif_indicators AS (
  SELECT
    *,
    tariforganismecommercial - tariforganismeadministratif AS ecart_tarif,
    CASE 
      WHEN tariforganismeadministratif > 0 
      THEN ((tariforganismecommercial - tariforganismeadministratif)/tariforganismeadministratif)*100 
      ELSE NULL 
    END AS pourcentage_ecart,
    CASE
      WHEN qtemax IS NULL THEN CONCAT('≥', qtemin)
      WHEN qtemin = qtemax THEN CAST(qtemin AS VARCHAR)
      ELSE CONCAT(qtemin, '-', qtemax)
    END AS plage_quantite,
    CASE 
      WHEN expiration THEN 'Expiré'
      ELSE 'Valide'
    END AS statut_tarif
  FROM tarif_data
)

-- Sélection finale avec toutes les colonnes
SELECT
  idtarif AS tarif_key,
  idtypeanalyse AS analyse_key,
  idtypetarif AS type_tarif_key,
  idunitemesure AS unite_mesure_key,
  idlaboratoire AS laboratoire_key,
  type_analyse,
  description_analyse,
  type_tarif_code,
  type_tarif_libelle,
  type_tarif_ordre,
  type_tarif_qte_groupe,
  unite_mesure_code,
  unite_mesure_libelle,
  unite_mesure_ordre,
  unite_mesure_valeur,
  laboratoire_code,
  laboratoire_libelle,
  tariforganismeadministratif AS tarif_administratif,
  tariforganismecommercial AS tarif_commercial,
  ecart_tarif,
  pourcentage_ecart,
  qtemin AS quantite_min,
  qtemax AS quantite_max,
  plage_quantite,
  qteengroupe AS quantite_groupe,
  idqteamultiplierpar AS multiplicateur_quantite_key,
  statut_tarif,
  expiration AS est_expire,
  datedernieremodification AS date_mise_a_jour,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM tarif_indicators