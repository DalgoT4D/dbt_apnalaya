{% macro flatten_generic_json(table_name, row_id_column, json_column, array_field, prefix) %}
WITH expanded AS (
    SELECT 
        {{ row_id_column }} AS row_id,
        jsonb_each_text({{ json_column }}) AS top_level,  -- Extract top-level fields
        jsonb_array_elements({{ json_column }}->'{{ array_field }}') AS nested_data  -- Extract array elements
    FROM {{ table_name }}
)

-- flattened AS (
--     SELECT 
--         e.row_id,
--         -- Extract top-level fields dynamically
--         top_level.key AS top_field_key,
--         top_level.value AS top_field_value,
--         -- Extract all nested array fields dynamically
--         jsonb_each_text(nested_data) AS nested
--     FROM expanded e
-- )

SELECT * from expanded

-- SELECT 
--     row_id,
--     jsonb_object_agg(top_field_key, top_field_value) AS top_fields,
--     jsonb_object_agg(nested.key, nested.value) AS nested_fields
-- FROM flattened
-- GROUP BY row_id;
{% endmacro %}