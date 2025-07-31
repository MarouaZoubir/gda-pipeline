{{ config(
    materialized='table',
    schema='mart',
    tags=['mart', 'bonanalyse']
) }}

SELECT
    prof.idprofesseur,
    prof.nb_bonanalyse_total,
    prof.nb_bonanalyse_valides,
    prof.montant_total_alloue,
    prof.nb_statuts_utilises,
    CASE 
        WHEN prof.nb_bonanalyse_total > 1 THEN TRUE
        ELSE FALSE
    END AS utilise_plusieurs_bons
FROM {{ ref('int_bonanalyse_by_professeur') }} prof
