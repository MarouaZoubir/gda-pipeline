{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH professor_activity AS (
    SELECT
        dd.date_key,
        dp.id AS professor_key,
        deu.etablissement_universitaire_key AS university_key,
        COUNT(DISTINCT fb.dossier_analyse_key) AS dossier_count,
        COUNT(DISTINCT fb.bon_analyse_key) AS bon_analyse_count,
        SUM(fb.montant_bon_analyse) AS total_spend,
        COUNT(DISTINCT fb.etudiant_key) AS distinct_students,
        STRING_AGG(DISTINCT fb.types_analyses_presents, ', ') AS analysis_types
    FROM {{ ref('fact_analyses') }} fb
    JOIN {{ ref('dim_date') }} dd ON TO_CHAR(fb.date_creation_bon_analyse, 'YYYYMMDD')::INT = dd.date_key
    JOIN {{ ref('dim_professeur') }} dp ON fb.professeur_key = dp.id
    JOIN {{ ref('dim_etablissementuniversitaire') }} deu ON fb.etablissement_universitaire_key = deu.etablissement_universitaire_key
    GROUP BY 1, 2, 3
)

SELECT * FROM professor_activity