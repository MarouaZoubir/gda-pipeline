{{ config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'bonanalyse']
) }}

SELECT 
    idprofesseur,
    COUNT(DISTINCT idbonanalyse) AS nb_bonanalyse_total,
    SUM(montantinitial) AS montant_total_alloue,
    COUNT(CASE WHEN idstatutbonanalyse = 3 THEN 1 END) AS nb_bonanalyse_valides,
    COUNT(DISTINCT idstatutbonanalyse) AS nb_statuts_utilises
FROM {{ ref('stg_bonanalyse') }}
WHERE idprofesseur IS NOT NULL
GROUP BY idprofesseur
