{{ config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'aggregation']
) }}

SELECT
    iduser,
    COUNT(*) AS nb_modifications,
    COUNT(DISTINCT intitule) AS nb_categories_modifiees,
    MIN(dateoperation) AS first_modification,
    MAX(dateoperation) AS last_modification
FROM {{ ref('stg_history') }}
WHERE description ILIKE '%prestation%'
GROUP BY iduser
