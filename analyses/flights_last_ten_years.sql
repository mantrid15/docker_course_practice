{% set current_date = run_started_at|string|truncate(10, True, "") %}
{% set current_year = run_started_at|string|truncate(4, True, "")|int  %}
{% set prev_year = current_year - 10 %}

SELECT 
COUNT(*) as {{ adapter.quote('select') }}
FROM
    {{ ref('fct_flights') }}
WHERE 
    scheduled_departure BETWEEN '{{ current_date }}' AND '{{ current_date | replace(current_year, prev_year) }}'

{% set fct_flights = api.Relation.create(
    database="dwh_flight",
    schema="intermediate",
    identifier="fct_flights",
    type="table"
    )
%}

{% set stg_flights__flights = api.Relation.create(
    database="dwh_flight",
    schema="intermediate", 
    identifier="stg_flights__flights",
    type="table"
    )
%} 

{% do adapter.expand_target_column_types(stg_flights__flights, fct_flights) %}

{% for col in adapter.get_columns_in_relation(stg_flights__flights) %}
    {{ 'Column: ' ~ col }}
{%- endfor %} 
{% for col in adapter.get_columns_in_relation(fct_flights) %}
    {{ 'Column: ' ~ col }}  
{%- endfor %} 
{# 
{% set fct_flights = api.Relation.create(
    database="dwh_flight",
    schema="intermediate", 
    identifier="fct_flights"
) %}

{% set stg_flights = api.Relation.create(
    database="dwh_flight",
    schema="intermediate",
    identifier="stg_flights__flights"
) %}

{% set missing_columns = adapter.get_missing_columns(stg_flights, fct_flights) %}

{% if missing_columns %}
    {% for col in missing_columns %}
        -- Missing column: {{ col.name }} ({{ col.data_type }})
    {% endfor %}
{% else %}
    -- No missing columns
{% endif %} #}







{# {% do adapter.drop_schema(
    api.Relation.create(
        database="dwh_flight",
        schema="test_schema",
        )
    )
%} #}
{# 
{%- set source_relation = adapter.get_relation(  
    database="dwg_flight",
    schema="intermediate",
    identifier="fct_flights") %}
#}
    {# {%- set source_relation = load_relation(    ref('fct_bookings')) -%} #}


    {# {%- set source_relation = api.Relation.create(
        database="dwg_flight", 
        schema="intermediate", 
        data'fct_fligths',  
        type = "table") 
    %} #}



    {# {% set columns = adapter.get_columns_in_relation(source_relation) %} #}

    {# {% for col in columns -%}  
        {{ 'Columns:' ~ col }}
    {% endfor %}  #}

{# 
 -- Return the `database` for this relation
{{ source_relation.database }}

-- Return the `schema` (or dataset) for this relation
{{ source_relation.schema }}

-- Return the `identifier` for this relation
{{ source_relation.identifier }}

-- Return relation name without the database
{{ source_relation.include(database=false) }}

-- Return true if the relation is a table
{{ source_relation.is_table }}

-- Return true if the relation is a view
{{ source_relation.is_view }}

-- Return true if the relation is a cte
{{ source_relation.is_cte }} 
#}

{# {{ log("Source Relation: " ~ source_relation, info=true) }}  #}