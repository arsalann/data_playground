SELECT
    transaction_year,
    age_group,
    SUM(passage_cnt) AS total_passages,
    SUM(passenger_cnt) AS total_passengers
FROM `bruin-playground-arsalan.raw.istanbul_rail_age_group`
WHERE age_group IS NOT NULL AND age_group != ''
GROUP BY 1, 2
ORDER BY 1, 2
