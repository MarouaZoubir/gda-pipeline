{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH equipment_usage AS (
    SELECT
        dd.date_key,
        de.equipement_key,
        dl.idlaboratoire AS laboratoire_key,
        COUNT(DISTINCT fb.dossier_analyse_key) AS usage_count,
        COUNT(DISTINCT ta.idtypeanalyse) AS distinct_analysis_types,
        SUM(fb.montant_bon_analyse) AS generated_revenue
    FROM {{ ref('fact_analyses') }} fb
    JOIN {{ ref('dim_date') }} dd 
        ON TO_CHAR(fb.date_creation_bon_analyse, 'YYYYMMDD')::INT = dd.date_key
    JOIN {{ ref('dim_laboratoire') }} dl 
        ON fb.laboratoire_key = dl.idlaboratoire
    JOIN {{ ref('dim_equipement') }} de 
        ON EXISTS (
            SELECT 1 
            FROM UNNEST(de.analyses_ids) AS analysis_id
            JOIN {{ ref('dim_typeanalyse') }} ta 
                ON ta.idtypeanalyse = analysis_id
            WHERE ta.idlaboratoire = dl.idlaboratoire
        )
    JOIN {{ ref('dim_typeanalyse') }} ta 
        ON fb.types_analyses_presents LIKE '%' || ta.prestation || '%'
        AND ta.idlaboratoire = dl.idlaboratoire
    GROUP BY 1, 2, 3
)

SELECT * FROM equipment_usage