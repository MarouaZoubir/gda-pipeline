{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH invoice_performance AS (
    SELECT
        dd.date_key,
        df.idfacturation AS invoice_key,
        df.iddossieranalyse AS dossier_key,
        CASE 
            WHEN df.dtype = 'ADMINISTRATIVE' THEN 1
            WHEN df.dtype = 'COMMERCIAL' THEN 2
            ELSE 3
        END AS client_type_key,
        df.numero AS invoice_number,
        COUNT(DISTINCT fb.bon_analyse_key) AS bon_analyse_count,
        SUM(fb.montant_bon_analyse) AS total_amount,
        CASE 
            WHEN df.datedernieremodification_facture IS NULL THEN 'PENDING'
            ELSE 'PAID'
        END AS payment_status,
        (df.datedernieremodification_facture - df.datecreation) AS days_to_pay
    FROM {{ ref('dim_dossieranalyse') }} df
    JOIN {{ ref('dim_date') }} dd ON TO_CHAR(df.datecreation, 'YYYYMMDD')::INT = dd.date_key
    LEFT JOIN {{ ref('fact_analyses') }} fb ON df.iddossieranalyse = fb.dossier_analyse_key
    WHERE df.idfacturation IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, df.datedernieremodification_facture, df.datecreation
)

SELECT * FROM invoice_performance