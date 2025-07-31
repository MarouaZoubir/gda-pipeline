{% macro anonymize_signatures(signataires) %}
  {#
    Proper signature anonymization macro (expression-only version)
    Formats: "First1 Last1/First2 Last2" → "F*** L*** / F*** L***"
  #}
  CASE 
    WHEN {{ signataires }} IS NOT NULL THEN
      CONCAT(
        -- First name part 1
        LEFT(SPLIT_PART(SPLIT_PART({{ signataires }}, '/', 1), ' ', 1), 1),
        REPEAT('*', 3), ' ',
        -- Last name part 1
        LEFT(SPLIT_PART(SPLIT_PART({{ signataires }}, '/', 1), ' ', 2), 1),
        REPEAT('*', 3), ' / ',
        -- First name part 2
        LEFT(SPLIT_PART(SPLIT_PART({{ signataires }}, '/', 2), ' ', 1), 1),
        REPEAT('*', 3), ' ',
        -- Last name part 2
        LEFT(SPLIT_PART(SPLIT_PART({{ signataires }}, '/', 2), ' ', 2), 1),
        REPEAT('*', 3)
      )
    ELSE NULL
  END
{% endmacro %}