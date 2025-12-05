{% macro get_data_freshness(table_name, timestamp_column) %}
    
    -- Macro to calculate the time difference (in hours) between the most recent 
    -- record in a table and the current timestamp.

    {% set query %}
        SELECT 
            DATEDIFF(HOUR, MAX({{ timestamp_column }}), CURRENT_TIMESTAMP()) 
        FROM 
            {{ ref(table_name) }}
    {% endset %}

    -- Use the run_query function to execute the query and get the result
    {% set result = run_query(query) %}

    -- Return the first value from the query result
    {% if execute %}
        {% set freshness_hours = result.columns[0].values()[0] %}
        {{ return(freshness_hours) }}
    {% endif %}
    
    -- Fallback for parsing time
    {{ return(none) }}

{% endmacro %}
