{{
  config(
    materialized='table',
    schema='mart'
  )
}}
WITH dim_professeur AS (
    SELECT  
        me."idmembreexterne" AS id,
        cv.abreviation as civilite,
        concat(me.prenom_anon, ' ', me.nom_anon) AS "Professeur",
        me.masked_cin,
        me.hashed_email, 
        me.masked_phone, 
        me.masked_fax, 
        me.orcid,
        me.ville, 
        p.abreviation AS pays,
        eu."idetablissementuniversitaire",
        eu.nom AS EtablissementUniversitaire,
        fac."idfaculteecole", 
        concat(fac.nom, ' - ', eu.nom) AS faculte_ecole
    FROM 
        {{ ref('anonymized_membreexterne') }} me
    LEFT JOIN 
        {{ ref('stg_pays') }} p ON p."idpays" = me."idpays"
     LEFT JOIN 
        {{ ref('stg_civilite') }}cv ON cv."idcivilite" = me."idcivilite"
    INNER JOIN 
        {{ ref('stg_detailfaculteecoleprofesseur') }} dfacprof ON dfacprof."idprofesseur" = me."idmembreexterne"
    LEFT JOIN 
        {{ ref('anon_faculteecole') }} fac ON fac."idfaculteecole" = dfacprof."idfaculteecole"
    INNER JOIN 
        {{ ref('stg_etablissementuniversitaire') }} eu ON eu."idetablissementuniversitaire" = fac."idetablissementuniversitaire"
    WHERE 
        "dtype" like 'professeur'
)
SELECT * FROM dim_professeur
