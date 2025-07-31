{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH analysis_performance AS (
    SELECT
        dd.date_key,
        dta.idtypeanalyse AS analysis_type_key,
        dl.idlaboratoire AS laboratoire_key,
        COUNT(DISTINCT fb.dossier_analyse_key) AS request_count,
        SUM(CASE WHEN fb.types_analyses_presents LIKE '%' || dta.prestation || '%' THEN 1 ELSE 0 END) AS usage_count,
        AVG(dt.tarif_administratif) AS avg_admin_price,
        AVG(dt.tarif_commercial) AS avg_commercial_price
    FROM {{ ref('fact_analyses') }} fb
    JOIN {{ ref('dim_date') }} dd ON TO_CHAR(fb.date_creation_bon_analyse, 'YYYYMMDD')::INT = dd.date_key
    JOIN {{ ref('dim_typeanalyse') }} dta ON fb.types_analyses_presents LIKE '%' || dta.prestation || '%'
    JOIN {{ ref('dim_laboratoire') }} dl ON dta.idlaboratoire = dl.idlaboratoire
    LEFT JOIN {{ ref('dim_tariff') }} dt ON dta.idtypeanalyse = dt.analyse_key
    GROUP BY 1, 2, 3
)

SELECT * FROM analysis_performance