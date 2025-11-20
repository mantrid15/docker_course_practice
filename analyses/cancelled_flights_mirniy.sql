SELECT scheduled_departure::date as scheduled_departure,
count(*) as cancelled_flights
from 
    {{ ref('fct_flights') }}
    {# {{ ref('dim_flights__airports') }} #}
    {# MJZ #}  
Where 
    departure_airport = 'MJZ'
ANd status = 'Cancelled'
Group by
scheduled_departure::date