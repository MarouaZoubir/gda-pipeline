{{
  config(
    materialized='incremental',
    schema='analytics',
    unique_key='tariff_change_key',
    tags=['fact']
  )
}}

WITH tariff_changes AS (
  SELECT
    tarif_key,
    analyse_key,
    type_tarif_key,
    unite_mesure_key,
    laboratoire_key,
    tarif_administratif,
    tarif_commercial,
    ecart_tarif,
    pourcentage_ecart,
    quantite_min,
    quantite_max,
    plage_quantite,
    quantite_groupe,
    multiplicateur_quantite_key,
    statut_tarif,
    est_expire,
    date_mise_a_jour
  FROM {{ ref('dim_tariff') }}
  {% if is_incremental() %}
  WHERE date_mise_a_jour > (SELECT MAX(change_date) FROM {{ this }})
  {% endif %}
)

SELECT
  ROW_NUMBER() OVER (ORDER BY tc.tarif_key, tc.date_mise_a_jour) AS tariff_change_key,
  dd.date_key,
  tc.tarif_key,
  tc.analyse_key,
  tc.type_tarif_key,
  tc.unite_mesure_key,
  tc.laboratoire_key,
  CASE 
    WHEN tc.tarif_administratif IS NOT NULL THEN 1  -- Administrative
    WHEN tc.tarif_commercial IS NOT NULL THEN 2     -- Commercial
    ELSE 3                                         -- Other
  END AS client_type_key,
  tc.tarif_administratif,
  tc.tarif_commercial,
  tc.ecart_tarif,
  tc.pourcentage_ecart,
  tc.quantite_min,
  tc.quantite_max,
  tc.plage_quantite,
  tc.quantite_groupe,
  tc.multiplicateur_quantite_key,
  tc.statut_tarif,
  tc.est_expire,
  tc.date_mise_a_jour AS change_date,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM tariff_changes tc
LEFT JOIN {{ ref('dim_date') }} dd 
  ON TO_CHAR(tc.date_mise_a_jour, 'YYYYMMDD')::INT = dd.date_key