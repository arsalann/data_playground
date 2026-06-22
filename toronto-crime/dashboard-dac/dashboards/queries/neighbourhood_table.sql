SELECT
    rank_by_rate,
    rank_by_count,
    neighbourhood_name,
    crime_count,
    crime_count_per_1000_people,
    crime_count_per_km2,
    population,
    land_area_km2
FROM report.neighbourhood_rankings
WHERE rank_by_rate <= 25
ORDER BY rank_by_rate
