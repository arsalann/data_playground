{#
  Custom schema naming so `+schema: staging` on a model yields the BigQuery
  dataset `contoso_dbt_staging` (not `contoso_dbt_contoso_dbt_staging` which
  is dbt's default dataset-then-schema concatenation).

  Sources use their own `schema:` in sources.yml and are unaffected.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema ~ '_' ~ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
