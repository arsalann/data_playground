SELECT
    rank_by_rate,
    rank_by_count,
    neighbourhood_name,
    CONCAT(
        CAST(rank_by_rate AS STRING),
        ' ',
        CASE neighbourhood_name
            WHEN 'Mimico-Queensway' THEN 'Mimico'
            WHEN 'Yonge-Bay Corridor' THEN 'Yonge-Bay'
            WHEN 'Downtown Yonge East' THEN 'Downtown'
            WHEN 'Kensington-Chinatown' THEN 'Kensington'
            WHEN 'West Humber-Clairville' THEN 'W Humber'
            WHEN 'Yorkdale-Glen Park' THEN 'Yorkdale'
            WHEN 'York University Heights' THEN 'York U'
            ELSE REGEXP_REPLACE(neighbourhood_name, r'\s+', ' ')
        END
    ) AS rank_label,
    crime_count,
    crime_count_per_1000_people,
    crime_count_per_km2,
    citywide_rate_per_1000_people,
    population,
    land_area_km2,
    latest_complete_year
FROM report.neighbourhood_rankings
WHERE rank_by_rate <= 10
ORDER BY rank_by_rate
