SELECT
  query_airport,
  query_airport_region,
  COUNT(*) AS total_flights,
  COUNT(DISTINCT airline_icao) AS unique_airlines,
  COUNT(DISTINCT aircraft_type) AS unique_aircraft_types,
  ROUND(AVG(actual_distance_km), 0) AS avg_distance_km,
  ROUND(AVG(flight_duration_min), 1) AS avg_duration_min,
  ROUND(MAX(actual_distance_km), 0) AS max_distance_km,

  COUNTIF(manufacturer = 'Airbus') AS airbus_flights,
  COUNTIF(manufacturer = 'Boeing') AS boeing_flights,
  COUNTIF(manufacturer NOT IN ('Airbus', 'Boeing')) AS other_manufacturer_flights,

  COUNTIF(aircraft_size = 'Widebody') AS widebody_flights,
  COUNTIF(aircraft_size = 'Narrowbody') AS narrowbody_flights,
  COUNTIF(aircraft_size = 'Regional') AS regional_flights,

  COUNTIF(distance_category = 'Short-haul') AS short_haul,
  COUNTIF(distance_category = 'Medium-haul') AS medium_haul,
  COUNTIF(distance_category = 'Long-haul') AS long_haul,

  COUNTIF(flight_category = 'Passenger') AS passenger_flights,
  COUNTIF(flight_category = 'Cargo') AS cargo_flights,
  COUNTIF(flight_category NOT IN ('Passenger', 'Cargo')) AS other_category_flights,

  COUNTIF(is_diverted) AS diverted_flights

FROM `bruin-playground-arsalan.staging.flights_by_hub`
GROUP BY query_airport, query_airport_region
ORDER BY total_flights DESC
