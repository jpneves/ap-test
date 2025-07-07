{% macro delete_data(
    date_col
) %}

    {% if is_incremental() -%}

        -- Step 1: Remove rows with future dates based on the execution date.
        delete from {{ this }}
        where {{ date_col }} = to_date('{{ var("execution_date", "") }}', 'YYYY-MM-DD');

    {% endif %}

{% endmacro %}
