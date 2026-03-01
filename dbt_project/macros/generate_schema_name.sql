-- Override generate_schema_name so that custom schemas (staging, mart) are used
-- as-is, without being prefixed with the target schema from profiles.yml.
-- Without this, dbt would generate "stg_stg" instead of "stg".

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
