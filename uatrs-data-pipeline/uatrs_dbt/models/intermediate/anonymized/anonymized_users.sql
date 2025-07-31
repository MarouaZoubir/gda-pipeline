{{
  config(
    materialized='view',
    tags=['intermediate', 'pii_anonymized']
  )
}}

SELECT
  idUser,
  {{ hash_with_salt("userName") }} AS hashed_username,
  NULL AS password,
  enabled,
  initialized,
  ismentionlegalvalid,
  istested,
  iscompteactive
FROM {{ ref('stg_users') }}