{{
    config(
        materialized ='table'
        )
    
}}

SELECT  
    book_ref, 
    book_date,3
    total_amount
from {{ ref('stg_flights__bookings') }} 