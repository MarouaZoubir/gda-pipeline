{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_membreexterne AS (
    SELECT 
        me."dtype" AS ME_DType, 
        me."idmembreexterne", 
        me.hashed_email, 
        me.masked_cin, 
        CONCAT(me.prenom_anon, ' ', me.nom_anon) AS nom_complet, 
        me."iduser",
        me."idetablissementnonuniversitaire", 
        enu."dtype" AS enu_DType, 
        enu.nom AS etablissementnonuniversitaire, 
        senu.abreviation AS secteur_1,
        me."idetablissementcaractereadministratif", 
        eca.intitule AS etablissementcaractereadministratif, 
        te.abreviation AS TypeEtablissement, 
        me."idetablissementcaracterecommercial", 
        ec.abreviation AS EtablissementCaractereCommercial, 
        sec.abreviation AS secteur_2
    FROM 
        {{ ref('anonymized_membreexterne') }} me
    LEFT JOIN 
        {{ ref('stg_etablissementnonuniversitaire') }} enu 
        ON me."idetablissementnonuniversitaire"= enu."idetablissementnonuniversitaire"
    LEFT JOIN 
        {{ ref('anon_etablissementcaracterecommercial') }} ec 
        ON me."idetablissementcaracterecommercial"=ec."idetablissementcaracterecommercial"
    LEFT JOIN 
        {{ ref('anon_etablissementcaractereadministratif') }} eca 
        ON me."idetablissementcaractereadministratif" = eca."idetablissementcaractereadministratif"
    LEFT JOIN 
        {{ ref('stg_typeetablissement') }} te 
        ON te."idtypeetablissement" = eca."idtypeetablissement"
    LEFT JOIN 
        {{ ref('stg_secteur') }} senu 
        ON senu."idsecteur" = enu."idsecteur"  -- Fixed from "isecteur" to "idsecteur"
    LEFT JOIN 
        {{ ref('stg_secteur') }} sec 
        ON ec."idsecteur" = sec."idsecteur"  -- Fixed typo (was "idscteur")
    WHERE 
        me."dtype" LIKE 'particulier'
)
SELECT * FROM dim_membreexterne