{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_devis AS (
    SELECT DISTINCT
        d."iddevis",
        d."datecreation", 
        d."montantinitial", 
        d."numerodevis", 
        d."consomme",
        d."active",
        d."iddemandeuretablissementcaractereadministratif", d."iddemandeuretablissementcaracterecommercial",
        d."idparticulier",
        concat(me.prenom_anon, ' ', me.nom_anon) as nom_complet, 
        me.masked_cin, 
        me.hashed_email,
        
        dlu."idlaboratoire",
        dlu.laboratoire,
        dlu."idunitemesure" as idUniteMesure_lab,
        dlu.unitemesure,
        dtu."idtypeanalyse",
        dtu."prestation" AS typeAnalyse,
        dtu."idunitemesure" as idUniteMesure_type,
        dtu."abreviation" AS typeAnalyse_unitemesure,
        dd.valeur
    FROM 
        {{ ref('stg_devis') }} d
    left JOIN 
        {{ ref('anonymized_membreexterne') }} me ON d."idparticulier" = me."idmembreexterne"
    LEFT JOIN 
        {{ ref('stg_detailsdevis') }} dd ON dd."iddevis" = d."iddevis"
    left join 
    {{ref('dim_detail_labo_unitemesure')}} dlu on dlu."iddetaillaboratoireunitemesure" = dd."iddetaillaboratoireunitemesure"
    left join 
    {{ref('dim_detail_typeanalyse_unitemesure')}} dtu on dtu."iddetailtypeanalyseunitemesure" = dd."iddetailtypeanalyseunitemesure"
     WHERE 
      d."idparticulier" IS NOT NULL
)
SELECT * FROM dim_devis
