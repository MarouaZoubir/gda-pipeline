{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_dossieranalyse AS (
    SELECT 
        df."iddossieranalyse", 
        df."numerodossieranalyse",  
        df."datecreation", 
        df."datedernieremodification", 
        df."idbonanalyse", 
        df."iddevis", 
        df."idetudiant", 
        df."numerodemandeficheanalyse", 
        df."idlaboratoire", 
        df."suivieparuser",
        df."idfacturation", 
        f."dtype", 
        f."billing_date" as "datedernieremodification_facture",
        f.numero
    FROM 
        {{ ref('stg_dossieranalyse') }} df
    LEFT JOIN 
        {{ ref('anonymized_facturation') }} f ON f."idfacturation" = df."idfacturation"
    LEFT JOIN 
        {{ ref('stg_detaildossieranalyseficheanalyse') }} sda ON sda."iddossieranalyse" = df."iddossieranalyse"
)
SELECT * FROM dim_dossieranalyse