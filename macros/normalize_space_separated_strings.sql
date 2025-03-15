{% macro normalize_string(column) %}
  array_to_string(
    (
      SELECT array_agg(elem ORDER BY elem)
      FROM unnest(string_to_array({{ column }}, ' ')) AS elem
    ),
    ' '
  )
{% endmacro %}