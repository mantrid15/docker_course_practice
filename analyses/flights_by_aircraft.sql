{% set aircraft_query%}
select DISTINCT 
aircraft_code
from
{{ ref('fct_flights') }}
    {% endset %}

{% set aircraft_query_result =  run_query(aircraft_query)%}
{% if execute %}
    {% set importants_aircrafts = aircraft_query_result.columns[0].values()  %}
{% else %}
    {% set importants_aircrafts = [] %}
{% endif %}
select 
    {% for aircraft in importants_aircrafts %} 
    SUM(CASE when aircraft_code = '{{ aircraft }}' then 1 else 0 end) as flights_{{ aircraft }}
        {%- if not loop.last %}, 
        {% endif %}
    {%- endfor %}
from {{ ref('fct_flights') }}