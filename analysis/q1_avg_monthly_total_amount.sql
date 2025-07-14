-- Pergunta 1: Qual a média de valor total (total_amount) recebido em um mês 
-- considerando todos os yellow táxis da frota?

SELECT
    -- Calulate the average of the total value
    ROUND(AVG(total_amount), 2) AS average_total_amount,
    -- Include the sum total
    ROUND(SUM(total_amount), 2) AS total_revenue,
    -- Include the total number of trups
    COUNT(1) AS total_trips
FROM
    `workspace_ifood-case`.nyc_taxi.yellow_silver