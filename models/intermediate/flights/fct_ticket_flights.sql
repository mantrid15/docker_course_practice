{{
    config(
        materialized ='table'
        )
    
}}

SELECT  
    ticket_no,
    flight_id,
    fare_conditions,
    amount

from {{ ref('stg_flights__ticket_flights') }} 