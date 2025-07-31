SELECT 
    iddemandeuretablissementcaractereadministratif AS id_etablissement,
    COUNT(*) AS nb_devis_non_consomes,
    SUM(montantinitial) AS montant_total_non_consomme
FROM {{ ref('stg_devis') }}
WHERE consomme = false
  AND datecreation <= (CURRENT_DATE - INTERVAL '5 years')
GROUP BY iddemandeuretablissementcaractereadministratif
ORDER BY nb_devis_non_consomes DESC