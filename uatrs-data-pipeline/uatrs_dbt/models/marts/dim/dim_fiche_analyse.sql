WITH dim_detailsficheanalyse AS (
    SELECT DISTINCT
        dfa."iddetailsficheanalyse", 
        dfa."idbonanalyse", 
        dfa."iddevis", 
        dfa.valeur, 
        dfa."iddemandeficheanalyse", 
        dfa."isconsommee",
        scfa."idsouschampficheanalyse",
        scfa.abreviation AS sous_champ_FA_Abreviation, 
        vpfa_s.abreviation AS sous_champ_FA_Valeur_Proposee,
        dcfascfa."idchampficheanalyse",
        cfa.abreviation AS champ_fiche_analyse,
        vpfa_c.abreviation AS champ_FA_Valeur_Proposee,
        fa."idficheanalyse"
    FROM 
        {{ ref('stg_detailsficheanalyse') }} dfa
    LEFT JOIN 
        {{ ref('stg_detailsouschampficheanalysevaleurproposeeficheanalyse') }} dscfavpfa ON dscfavpfa."iddetailsouschampficheanalysevaleurproposeeficheanalyse" = dfa."iddetailsouschampficheanalysevaleurproposeeficheanalyse"
    LEFT JOIN 
        {{ ref('stg_souschampficheanalyse') }} scfa ON scfa."idsouschampficheanalyse" = dscfavpfa."idsouschampficheanalyse"
    LEFT JOIN 
        {{ ref('stg_valeurproposeeficheanalyse') }} vpfa_s ON vpfa_s."idvaleurproposeeficheanalyse" = dscfavpfa."idvaleurproposeeficheanalyse"
    LEFT JOIN 
        {{ ref('stg_typechamp') }} tc ON tc."idtypechamp" = dscfavpfa."idtypechamp"
    LEFT JOIN 
        {{ ref('stg_detailchampficheanalysesouschampficheanalyse') }} dcfascfa ON dcfascfa."idsouschampficheanalyse" = scfa."idsouschampficheanalyse"
    INNER JOIN 
        {{ ref('stg_champficheanalyse') }} cfa ON cfa."idchampficheanalyse" = dcfascfa."idchampficheanalyse"
    INNER JOIN 
        {{ ref('stg_detailchampficheanalysevaleurproposeeficheanalyse') }} dcfavpfa ON dcfavpfa."iddetailchampficheanalysevaleurproposeeficheanalyse" = cfa."idchampficheanalyse"
    LEFT JOIN 
        {{ ref('stg_valeurproposeeficheanalyse') }} vpfa_c ON vpfa_c."idvaleurproposeeficheanalyse" = dcfavpfa."idvaleurproposeeficheanalyse"
    LEFT JOIN 
        {{ ref('stg_ficheanalyse') }} fa ON fa."idficheanalyse" = dcfavpfa."idficheanalyse"
)
SELECT * FROM dim_detailsficheanalyse
