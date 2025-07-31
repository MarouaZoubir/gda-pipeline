{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH lab_utilization AS (
    SELECT
        dd.date_key,
        dl.idlaboratoire AS laboratoire_key,
        COUNT(DISTINCT fb.dossier_analyse_key) AS analysis_count,
        COUNT(DISTINCT CASE WHEN fb.analyses_elements_chimiques > 0 THEN fb.dossier_analyse_key END) AS chemical_analysis_count,
        COUNT(DISTINCT CASE WHEN fb.analyses_morphologiques > 0 THEN fb.dossier_analyse_key END) AS morphology_analysis_count,
        SUM(fb.analyses_numeriques_total) AS numeric_analysis_total,
        COUNT(DISTINCT fb.professeur_key) AS distinct_professors,
        COUNT(DISTINCT fb.etudiant_key) AS distinct_students,
        SUM(fb.montant_bon_analyse) AS total_revenue
    FROM {{ ref('fact_analyses') }} fb
    JOIN {{ ref('dim_date') }} dd ON TO_CHAR(fb.date_creation_bon_analyse, 'YYYYMMDD')::INT = dd.date_key
    JOIN {{ ref('dim_laboratoire') }} dl ON fb.laboratoire_key = dl.idlaboratoire
    GROUP BY 1, 2
)

SELECT * FROM lab_utilization