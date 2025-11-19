{{
    config(
        materialized ='table'
        )
    
}}

SELECT  
    flight_id, 
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    {# departure_airport_id, #}
    {# arrival_airport_id, #}
    status,
    {# aircraft_id, #}
    actual_departure,
    actual_arrival
    {# load_date #}

from {{ ref('stg_flights__flights') }} 