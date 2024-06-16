{% macro generate_surrogate_key(args) %}
    md5(
        array_to_string(
            array[
                {% for arg in args %}
                    {{ arg }}::text,
                {% endfor %}
                ''
            ], 
            '||'
        )
    )
{% endmacro %}
