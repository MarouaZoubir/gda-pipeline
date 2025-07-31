{% macro anonymize(column_name, method='mask', show_last=4) %}
  {#
    Bias-free anonymization macro:
    - mask: Shows only the last N characters (default 4)
    - hash: Securely hashes the value
    - null: Returns NULL
    (Generalize method removed to avoid bias)
  #}
  {% if method == 'mask' %}
    CASE 
      WHEN {{ column_name }} IS NOT NULL AND {{ column_name }}::text != '' 
      THEN 'MASKED-' || RIGHT({{ column_name }}::text, {{ show_last }})
      ELSE NULL 
    END
  
  {% elif method == 'hash' %}
    {{ hash_with_salt(column_name) }}
  
  {% elif method == 'null' %}
    NULL
  
  {% else %}
    {{ exceptions.raise_compiler_error("Invalid anonymization method: " ~ method) }}
  {% endif %}
{% endmacro %}