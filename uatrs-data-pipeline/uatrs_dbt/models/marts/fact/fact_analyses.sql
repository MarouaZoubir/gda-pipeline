{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact']
  )
}}

WITH fact_analyses AS (
    SELECT
        -- Degenerate dimensions
        da."iddossieranalyse" AS dossier_analyse_key,
        da."numerodossieranalyse",
        ba."idbonanalyse" AS bon_analyse_key,
        ba."numeroba",
        d."iddevis" AS devis_key,
        d."numerodevis",
        
        -- Date dimensions
        ba."datecreation" AS date_creation_bon_analyse,
        d."datecreation" AS date_creation_devis,
        da."datecreation" AS date_creation_dossier,
        da."datedernieremodification" AS date_modification_dossier,
        
        -- Foreign keys to dimensions
        ba."idprofesseur" AS professeur_key,
        ba."idetablissementuniversitaire" AS etablissement_universitaire_key,
        ba."idfaculteecole" AS faculte_ecole_key,
        da."idetudiant" AS etudiant_key,
        da."idlaboratoire" AS laboratoire_key,
        dfa."iddemandeficheanalyse" AS demande_fiche_analyse_key,
        
        -- Measures
        ba."montantinitial" AS montant_bon_analyse,
        d."montantinitial" AS montant_devis,
        COUNT(DISTINCT fa."idficheanalyse") AS nombre_fiches_analyse,
        SUM(CASE WHEN fa."isconsommee" = TRUE THEN 1 ELSE 0 END) AS nombre_fiches_consommees,
        
        -- Proper handling of mixed valeur data
        COUNT(CASE WHEN fa.valeur IN ('Ag', 'Zr', 'P', 'Ti', 'Na') THEN 1 ELSE NULL END) AS analyses_elements_chimiques,
        COUNT(CASE WHEN fa.valeur IN ('Morphologie', 'Taille') THEN 1 ELSE NULL END) AS analyses_morphologiques,
        SUM(CASE WHEN fa.valeur ~ '^[0-9\.]+$' THEN CAST(fa.valeur AS NUMERIC) ELSE 0 END) AS analyses_numeriques_total,
        STRING_AGG(DISTINCT fa.valeur, ', ') AS types_analyses_presents
    FROM 
        {{ ref('dim_dossieranalyse') }} da
    LEFT JOIN 
        {{ ref('dim_bonanalyse') }} ba ON da."idbonanalyse" = ba."idbonanalyse"
    LEFT JOIN 
        {{ ref('dim_devis') }} d ON da."iddevis" = d."iddevis"
    LEFT JOIN 
        {{ ref('dim_demandeficheanalyse') }} dfa ON da."numerodemandeficheanalyse" = dfa."iddemandeficheanalyse"
    LEFT JOIN 
        {{ ref('dim_fiche_analyse') }} fa ON fa."iddemandeficheanalyse" = dfa."iddemandeficheanalyse"
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
)
SELECT * FROM fact_analyses