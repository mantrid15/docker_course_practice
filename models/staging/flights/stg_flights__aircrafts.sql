
-- This query selects all columns and all rows from the aircrafts table
-- The table is referenced using the source macro which points to the 'aircrafts' table in the 'demo_src' schema
-- Using the source macro allows for better lineage tracking in dbt

-- language=sql

{{ 
    config(materialized = 'table') 
}}

select
    aircraft_code,
    model,
    "range"
from
    {{ source('demo_src', 'aircrafts') }}