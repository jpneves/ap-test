{% macro generate_schema_name(custom_schema_name, node) -%}

    {% set path = node.path %}
    {%- set default_schema = target.schema -%}


    {%- if custom_schema_name is none -%}

        {% set data_source = path.split('/')[-2] %}
        {{ data_source | trim }}
    
    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

