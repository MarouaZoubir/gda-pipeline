{{
  config(
    materialized='table',
    schema='analytics',
    unique_key='unit_key',
    tags=['dimension']
  )
}}

SELECT
  idunitemesure AS unit_key,
  abreviation AS unit_code,
  intitule AS unit_name,
  ordre AS unit_order,
  valeur AS unit_value,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM {{ ref('stg_unitemesure') }}