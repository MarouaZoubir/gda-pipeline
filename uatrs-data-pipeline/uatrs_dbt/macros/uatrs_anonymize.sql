{% macro uatrs_mask_phone(column_name) %}
  CASE
    WHEN {{ column_name }} IS NOT NULL THEN 
      CASE 
        WHEN {{ column_name }}::text LIKE '+212%' THEN CONCAT('+*** ', RIGHT({{ column_name }}::text, 2))
        WHEN {{ column_name }}::text ~ '^\d+$' THEN CONCAT('+*** ', RIGHT({{ column_name }}::text, 2))
        ELSE '***-***-**' 
      END
    ELSE NULL
  END
{% endmacro %}

{% macro uatrs_generalize_establishment(column_name) %}
  CASE
    WHEN {{ column_name }} IN ('Université Hassan II', 'Université Mohammed V') THEN 'Major University'
    WHEN {{ column_name }} IN ('Ecole Nationale Supérieure', 'Ecole Nationale de Commerce') THEN 'Grande Ecole'
    ELSE 'Other Institution'
  END
{% endmacro %}