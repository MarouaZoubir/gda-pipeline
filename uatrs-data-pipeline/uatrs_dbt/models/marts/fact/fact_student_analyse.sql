{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH student_history AS (
    SELECT
        dd.date_key,
        de.etdId AS student_key,
        de.diplome AS degree_key,
        de.EtablissementUniversitaire AS university_key,
        COUNT(DISTINCT fb.dossier_analyse_key) AS analysis_count,
        COUNT(DISTINCT CASE WHEN fb.analyses_elements_chimiques > 0 THEN fb.dossier_analyse_key END) AS chemical_analysis_count,
        SUM(fb.montant_bon_analyse) AS total_cost,
        MIN(fb.date_creation_dossier) AS first_analysis_date,
        MAX(fb.date_creation_dossier) AS last_analysis_date
    FROM {{ ref('fact_analyses') }} fb
    JOIN {{ ref('dim_date') }} dd ON TO_CHAR(fb.date_creation_dossier, 'YYYYMMDD')::INT = dd.date_key
    JOIN {{ ref('dim_etudiant') }} de ON fb.etudiant_key = de.etdId
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM student_history