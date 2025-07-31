{{
  config(
    materialized='incremental',
    unique_key='iddemandeficheanalyse',
    schema='intermediate',
    tags=['intermediate', 'pii_anonymized']
  )
}}

{# Get the maximum existing date safely for incremental loads #}
{% if is_incremental() %}
    {% set max_date_query %}
        SELECT COALESCE(MAX(datedernieremodification), '1900-01-01'::timestamp) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

WITH source_data AS (
    SELECT
        iddemandeficheanalyse,
        datedernieremodification,
        numerodemandeficheanalyse,
        idspecialite,
        iddiplome,
        idmoyenpaiement,
        idstatutdemandeficheanalyse,  -- This column was missing!
        idbonanalyse,
        iddevis
    FROM {{ ref('stg_demandeficheanalyse') }}
    {% if is_incremental() %}
    WHERE datedernieremodification > '{{ max_date }}'
    {% endif %}
)

SELECT
    iddemandeficheanalyse,
    datedernieremodification,
    {{ dbt_utils.surrogate_key(['numerodemandeficheanalyse', 'iddemandeficheanalyse']) }} 
        AS anonymized_demand_number,
    {{ anonymize('numerodemandeficheanalyse', 'mask', 4) }} AS masked_demand_number,
    idspecialite,
    iddiplome,
    idmoyenpaiement,
    idstatutdemandeficheanalyse,  -- Added this column
    idbonanalyse,
    iddevis
FROM source_data