{% macro hash_with_salt(column_name, salt_var='ANONYMIZE_SALT') %}
  {#
    Secure hashing with salt that works across databases
    Args:
        column_name: The column to hash
        salt_var: Name of the variable containing the salt (default: 'ANONYMIZE_SALT')
  #}
  {{ return(adapter.dispatch('hash_with_salt', 'custom')(column_name, salt_var)) }}
{% endmacro %}

{% macro postgres__hash_with_salt(column_name, salt_var) %}
  {# PostgreSQL implementation using pgcrypto #}
  ENCODE(DIGEST(
    COALESCE({{ column_name }}::text, '') || 
    '{{ var(salt_var, "default-salt-123") }}', 
    'sha256'
  ), 'hex')
{% endmacro %}

{% macro default__hash_with_salt(column_name, salt_var) %}
  {# Fallback implementation using dbt_utils #}
  {{ dbt_utils.hash([
    column_name,
    var(salt_var, "default-salt-123")
  ]) }}
{% endmacro %}