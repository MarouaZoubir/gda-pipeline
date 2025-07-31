{{
  config(
    materialized='table',
    tags=['intermediate', 'pii_anonymized']
  )
}}

SELECT
  idfacturation,
  DATE(datecreation) AS billing_date,
  NULL AS rounded_amount,
  CASE
    WHEN numero IS NOT NULL THEN 
      'INV-' || EXTRACT(YEAR FROM DATE(datecreation))::text || '-' || 
      {{ anonymize('numero', 'mask', 4) }}
    ELSE NULL
  END AS masked_invoice_number,
  dtype,
  numero
FROM {{ ref('stg_facturation') }}