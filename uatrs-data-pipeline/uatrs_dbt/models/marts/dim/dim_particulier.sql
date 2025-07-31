{{
  config(
    materialized='table',
    schema='analytics'
  )
}}
WITH dim_particulier AS (
    SELECT DISTINCT
        me."idmembreexterne",
        cv.abreviation AS civilite,
        concat(me.prenom_anon, ' ', me.nom_anon) AS Particulier,
        me.masked_cin,
        me.hashed_email,
        me.ville,
        p.abreviation AS pays,
        me.masked_phone, 
        me.masked_fax, 
        me.orcid, 
        me."datedebutaffectation", 
        me."datefinaffectation", 
        me."isactive",
        sp.abreviation AS specialite,
        me."idetablissementcaractereadministratif", 
        me.dtype,
        me."idetablissementnonuniversitaire"
    FROM 
        {{ ref('anonymized_membreexterne') }} me
    LEFT JOIN 
        {{ ref('stg_pays') }} p ON p."idpays" = me."idpays"
    LEFT JOIN 
        {{ ref('stg_civilite') }} cv ON cv."idcivilite" = me."idcivilite"
    LEFT JOIN 
        {{ ref('stg_specialite') }} sp ON sp."idspecialite" = me."idspecialite"
    WHERE 
         "dtype" like 'particulier'
)
SELECT * FROM dim_particulier