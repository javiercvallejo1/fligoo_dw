CREATE SCHEMA processed;


CREATE TABLE IF NOT EXISTS processed.testdata(

    flight_date VARCHAR(100),
    flight_status VARCHAR(100),
    departure_airport VARCHAR(100),
    departure_timezone VARCHAR(100),
    arrival_airport VARCHAR(100),
    arrival_timezone VARCHAR(100),
    arrival_terminal VARCHAR(100), 
    airline_name VARCHAR(100),
    flight_number VARCHAR(100),
    timestamp DATE
)
PARTITION BY RANGE (timestamp);


