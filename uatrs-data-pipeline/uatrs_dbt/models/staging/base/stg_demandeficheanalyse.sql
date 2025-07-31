{{
    config(
        materialized='incremental',
        schema='staging',
        tags=['staging', 'raw'],
        unique_key='iddemandeficheanalyse'
    )
}}

{% if is_incremental() %}
    {# Safe approach to handle first run and potential NULL values #}
    {% set max_date_query %}
        SELECT COALESCE(MAX(datedernieremodification), '1900-01-01'::timestamp) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

SELECT
    iddemandeficheanalyse,
    datedernieremodification,
    idspecialite,
    iddiplome,
    idmoyenpaiement,
    idbonanalyse,
    iddevis,
    numerodemandeficheanalyse,
    idstatutdemandeficheanalyse
FROM {{ source('gda_raw', 'demandeficheanalyse') }}
{% if is_incremental() %}
WHERE datedernieremodification > '{{ max_date }}'
{% endif %}