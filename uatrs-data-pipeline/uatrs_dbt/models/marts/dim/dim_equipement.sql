{{
  config(
    materialized='table',
    unique_key='equipement_key',
    schema='analytics',
    tags=['dimension']
  )
}}

WITH equipement_base AS (
  SELECT
    e.idequipement,
    e.designation,
    e.marque,
    e.reference,
    e.natureequipement,
    e.ordre AS equipement_ordre,
    e.idstatutequipement,
    se.abreviation AS statut_equipement,
    se.intitule AS description_statut,
    e.datedernieremodification
  FROM {{ ref('stg_equipement') }} e
  LEFT JOIN {{ ref('stg_statutequipement') }} se ON e.idstatutequipement = se.idstatutequipement
),

equipement_analyses AS (
  SELECT
    eta.idequipement,
    ARRAY_AGG(DISTINCT ta.idtypeanalyse) AS analyses_ids,
    ARRAY_AGG(DISTINCT ta.prestation) AS analyses_noms,
    ARRAY_AGG(DISTINCT ta.intitule) AS analyses_descriptions,
    ARRAY_AGG(DISTINCT l.idlaboratoire) AS laboratoires_ids,
    ARRAY_AGG(DISTINCT l.abreviation) AS laboratoires_codes,
    ARRAY_AGG(DISTINCT l.intitule) AS laboratoires_noms
  FROM {{ ref('stg_detailequipementtypeanalyse') }} eta
  JOIN {{ ref('stg_typeanalyse') }} ta ON eta.idtypeanalyse = ta.idtypeanalyse
  JOIN {{ ref('stg_laboratoire') }} l ON ta.idlaboratoire = l.idlaboratoire
  GROUP BY eta.idequipement
),

equipement_maintenance AS (
  SELECT
    idequipement,
    MAX(datedernieremodification) AS derniere_maintenance_date,
    COUNT(*) AS nombre_maintenances,
    BOOL_OR(isactive) AS en_maintenance_active
  FROM {{ ref('stg_detailchargedemaintenanceequipement') }}
  GROUP BY idequipement
)

SELECT
  eb.idequipement AS equipement_key,
  eb.designation,
  eb.marque,
  eb.reference,
  eb.natureequipement,
  eb.equipement_ordre,
  eb.statut_equipement,
  eb.description_statut,
  
  -- Analysis information
  ea.analyses_ids,
  ea.analyses_noms,
  ea.analyses_descriptions,
  
  -- Laboratory information
  ea.laboratoires_ids,
  ea.laboratoires_codes,
  ea.laboratoires_noms,
  
  -- Maintenance information
  em.derniere_maintenance_date,
  em.nombre_maintenances,
  em.en_maintenance_active,
  
  -- Metadata
  eb.datedernieremodification AS derniere_modification_date,
  CURRENT_TIMESTAMP AS dbt_loaded_at
FROM equipement_base eb
LEFT JOIN equipement_analyses ea ON eb.idequipement = ea.idequipement
LEFT JOIN equipement_maintenance em ON eb.idequipement = em.idequipement