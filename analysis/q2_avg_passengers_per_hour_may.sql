-- Pergunta 2: Qual a média de passageiros (passenger_count) por cada hora do dia 
-- que pegaram táxi no mês de maio considerando todos os táxis da frota?

-- Create CTE with union between yellow and green results
-- Select passagers and trip datetime from yellow table
WITH trips_may AS (
    SELECT
        pickup_datetime,
        passenger_count
    FROM
        `workspace_ifood-case`.nyc_taxi.yellow_silver
    WHERE
        year = 2023 AND month = 5

    UNION ALL

    -- Select passagers and trip datetime from green table 
    SELECT
        pickup_datetime,
        passenger_count
    FROM
        `workspace_ifood-case`.nyc_taxi.green_silver
    WHERE
        year = 2023 AND month = 5
)

-- Final query
SELECT
    -- Extract the hour from the pickup_datetime column to group by hour of the day
    HOUR(pickup_datetime) AS hour_of_day,
    -- Calculate the average of passenger e round to 2 decimal places
    ROUND(AVG(passenger_count), 2) AS average_passengers,
    -- Count the total number of trips per hour for context
    COUNT(1) AS total_trips
FROM
    trips_may
GROUP BY
    hour_of_day
ORDER BY
    hour_of_day;
