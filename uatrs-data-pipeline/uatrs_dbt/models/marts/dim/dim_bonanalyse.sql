{{
  config(
    materialized='table',
    schema='mart'
  )
}}
WITH dim_bonanalyse AS (
    SELECT DISTINCT
        b."idbonanalyse", 
        b."datecreation", 
        b."datestatut", 
        b."isactuel", 
        b."active",
        b."montantinitial", 
        b."numeroba", 
        b."idprofesseur",
        concat(me."prenom_anon", ' ', me."nom_anon") AS professeur,
        me."masked_cin",
        eu."idetablissementuniversitaire",
        eu."nom" AS etablissement_universitaire,
        dfac."idfaculteecole",
        concat(fac."nom", ' - ', eu."nom") AS faculteecole_nom,
        sba."abreviation" as statut,
        scdba."abreviation" as statut_confirmation, 
        us."username" as suiviePar,
        -- dba."idDetailsBonAnalyse",
        dlu."idlaboratoire",
        dlu.laboratoire,
        dlu."idunitemesure" as idUniteMesure_lab,
        dlu.unitemesure,
        dtu."idtypeanalyse",
        dtu."prestation" AS typeAnalyse,
        dtu."idunitemesure" as idUniteMesure_type,
        dtu."abreviation" AS typeAnalyse_unitemesure,
        dba."valeur"
    FROM 
        {{ ref('stg_bonanalyse') }} b
    INNER JOIN 
        {{ ref('anonymized_membreexterne') }} me ON b."idprofesseur" = me."idmembreexterne"
    INNER JOIN 
        {{ ref('stg_detailfaculteecoleprofesseur') }} dfac ON me."idmembreexterne" = dfac."idprofesseur"
    INNER JOIN 
        {{ ref('anon_faculteecole') }} fac ON dfac."idfaculteecole" = fac."idfaculteecole"
    INNER JOIN 
        {{ ref('stg_etablissementuniversitaire') }} eu ON fac."idetablissementuniversitaire" = eu."idetablissementuniversitaire"
    INNER JOIN 
        {{ ref('stg_statutconfirmationdemandebonanalyse') }} scdba ON scdba."idstatutconfirmationdemandebonanalyse" = b."idstatutconfirmationdemandebonanalyse"
    INNER JOIN 
        {{ ref('stg_statutbonanalyse') }} sba ON sba."idstatutbonanalyse" = b."idstatutbonanalyse"
    LEFT JOIN 
        {{ ref('stg_users') }} us ON us."iduser" = b."suivieparuser"
    LEFT JOIN 
        {{ ref('stg_detailsbonanalyse') }} dba ON dba."idbonanalyse" = b."idbonanalyse"
    left join 
    {{ref('dim_detail_labo_unitemesure')}} dlu on dlu."iddetaillaboratoireunitemesure" = dba."iddetaillaboratoireunitemesure"
    left join 
    {{ref('dim_detail_typeanalyse_unitemesure')}} dtu on dtu."iddetailtypeanalyseunitemesure" = dba."iddetailtypeanalyseunitemesure"
    WHERE 
        b."idprofesseur" IS NOT NULL 
        AND dfac."idfaculteecole" IS NOT NULL
)
SELECT * FROM dim_bonanalyse
