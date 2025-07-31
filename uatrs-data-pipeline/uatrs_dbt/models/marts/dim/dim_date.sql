{{
  config(
    materialized='table',
    schema='analytics',
    tags=['dimension']
  )
}}

WITH date_range AS (
  SELECT 
    MIN(datecreation::date) AS min_date,
    MAX(datecreation::date) AS max_date
  FROM {{ ref('stg_bonanalyse') }}
),

date_series AS (
  SELECT
    (min_date + (n || ' day')::interval)::date AS date_value
  FROM date_range,
  generate_series(0, (max_date - min_date)) AS n
)

SELECT
  TO_CHAR(date_value, 'YYYYMMDD')::integer AS date_key,
  date_value AS date_complete,
  EXTRACT(DAY FROM date_value) AS jour,
  EXTRACT(MONTH FROM date_value) AS mois,
  EXTRACT(YEAR FROM date_value) AS annee,
  EXTRACT(QUARTER FROM date_value) AS trimestre,
  EXTRACT(WEEK FROM date_value) AS semaine,
  EXTRACT(DOW FROM date_value) AS jour_semaine,
  CASE 
    WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN FALSE  -- 0=Sunday, 6=Saturday
    ELSE TRUE 
  END AS est_jour_ouvrable,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM date_series