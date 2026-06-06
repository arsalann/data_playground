/* @bruin
name: raw.us_markets
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Synthetic US apparel market dimension used by the generated ecommerce,
  web analytics, and marketing raw assets. Covers the 50 US states plus DC
  with named city markets, regional labels, demand weights, and local tax
  rates for deterministic fake-data generation.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: market_id
    type: INTEGER
    description: Stable synthetic market identifier.
    primary_key: true
    nullable: false
  - name: city
    type: VARCHAR
    description: US city represented by the market row.
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation.
  - name: state_name
    type: VARCHAR
    description: Full US state or district name.
  - name: region
    type: VARCHAR
    description: US Census-style region used for apparel demand seasonality.
  - name: demand_weight
    type: DOUBLE
    description: Relative market demand weight used to scale orders, sessions, and spend.
  - name: sales_tax_rate
    type: DOUBLE
    description: Approximate local sales tax rate used for synthetic order tax calculations.

@bruin */

SELECT *
FROM UNNEST([
    STRUCT(1 AS market_id, 'New York' AS city, 'NY' AS state_code, 'New York' AS state_name, 'Northeast' AS region, 10.75 AS demand_weight, 0.0888 AS sales_tax_rate),
    STRUCT(2 AS market_id, 'Los Angeles' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 7.50 AS demand_weight, 0.0950 AS sales_tax_rate),
    STRUCT(3 AS market_id, 'Chicago' AS city, 'IL' AS state_code, 'Illinois' AS state_name, 'Midwest' AS region, 4.40 AS demand_weight, 0.1025 AS sales_tax_rate),
    STRUCT(4 AS market_id, 'Houston' AS city, 'TX' AS state_code, 'Texas' AS state_name, 'South' AS region, 3.80 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(5 AS market_id, 'Phoenix' AS city, 'AZ' AS state_code, 'Arizona' AS state_name, 'West' AS region, 3.00 AS demand_weight, 0.0860 AS sales_tax_rate),
    STRUCT(6 AS market_id, 'Philadelphia' AS city, 'PA' AS state_code, 'Pennsylvania' AS state_name, 'Northeast' AS region, 2.90 AS demand_weight, 0.0800 AS sales_tax_rate),
    STRUCT(7 AS market_id, 'San Antonio' AS city, 'TX' AS state_code, 'Texas' AS state_name, 'South' AS region, 2.40 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(8 AS market_id, 'San Diego' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 2.60 AS demand_weight, 0.0775 AS sales_tax_rate),
    STRUCT(9 AS market_id, 'Dallas' AS city, 'TX' AS state_code, 'Texas' AS state_name, 'South' AS region, 3.30 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(10 AS market_id, 'San Jose' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 1.80 AS demand_weight, 0.0925 AS sales_tax_rate),
    STRUCT(11 AS market_id, 'Austin' AS city, 'TX' AS state_code, 'Texas' AS state_name, 'South' AS region, 2.20 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(12 AS market_id, 'Jacksonville' AS city, 'FL' AS state_code, 'Florida' AS state_name, 'South' AS region, 1.60 AS demand_weight, 0.0750 AS sales_tax_rate),
    STRUCT(13 AS market_id, 'Fort Worth' AS city, 'TX' AS state_code, 'Texas' AS state_name, 'South' AS region, 1.80 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(14 AS market_id, 'Columbus' AS city, 'OH' AS state_code, 'Ohio' AS state_name, 'Midwest' AS region, 1.70 AS demand_weight, 0.0750 AS sales_tax_rate),
    STRUCT(15 AS market_id, 'Charlotte' AS city, 'NC' AS state_code, 'North Carolina' AS state_name, 'South' AS region, 2.00 AS demand_weight, 0.0725 AS sales_tax_rate),
    STRUCT(16 AS market_id, 'San Francisco' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 2.40 AS demand_weight, 0.0863 AS sales_tax_rate),
    STRUCT(17 AS market_id, 'Indianapolis' AS city, 'IN' AS state_code, 'Indiana' AS state_name, 'Midwest' AS region, 1.30 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(18 AS market_id, 'Seattle' AS city, 'WA' AS state_code, 'Washington' AS state_name, 'West' AS region, 2.20 AS demand_weight, 0.1010 AS sales_tax_rate),
    STRUCT(19 AS market_id, 'Denver' AS city, 'CO' AS state_code, 'Colorado' AS state_name, 'West' AS region, 2.00 AS demand_weight, 0.0881 AS sales_tax_rate),
    STRUCT(20 AS market_id, 'Washington' AS city, 'DC' AS state_code, 'District of Columbia' AS state_name, 'South' AS region, 2.10 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(21 AS market_id, 'Boston' AS city, 'MA' AS state_code, 'Massachusetts' AS state_name, 'Northeast' AS region, 2.00 AS demand_weight, 0.0625 AS sales_tax_rate),
    STRUCT(22 AS market_id, 'Nashville' AS city, 'TN' AS state_code, 'Tennessee' AS state_name, 'South' AS region, 1.40 AS demand_weight, 0.0925 AS sales_tax_rate),
    STRUCT(23 AS market_id, 'Baltimore' AS city, 'MD' AS state_code, 'Maryland' AS state_name, 'South' AS region, 1.20 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(24 AS market_id, 'Oklahoma City' AS city, 'OK' AS state_code, 'Oklahoma' AS state_name, 'South' AS region, 0.90 AS demand_weight, 0.0863 AS sales_tax_rate),
    STRUCT(25 AS market_id, 'Louisville' AS city, 'KY' AS state_code, 'Kentucky' AS state_name, 'South' AS region, 0.80 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(26 AS market_id, 'Portland' AS city, 'OR' AS state_code, 'Oregon' AS state_name, 'West' AS region, 1.30 AS demand_weight, 0.0000 AS sales_tax_rate),
    STRUCT(27 AS market_id, 'Las Vegas' AS city, 'NV' AS state_code, 'Nevada' AS state_name, 'West' AS region, 1.50 AS demand_weight, 0.0838 AS sales_tax_rate),
    STRUCT(28 AS market_id, 'Detroit' AS city, 'MI' AS state_code, 'Michigan' AS state_name, 'Midwest' AS region, 1.40 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(29 AS market_id, 'Memphis' AS city, 'TN' AS state_code, 'Tennessee' AS state_name, 'South' AS region, 0.80 AS demand_weight, 0.0975 AS sales_tax_rate),
    STRUCT(30 AS market_id, 'Milwaukee' AS city, 'WI' AS state_code, 'Wisconsin' AS state_name, 'Midwest' AS region, 0.90 AS demand_weight, 0.0590 AS sales_tax_rate),
    STRUCT(31 AS market_id, 'Albuquerque' AS city, 'NM' AS state_code, 'New Mexico' AS state_name, 'West' AS region, 0.60 AS demand_weight, 0.0788 AS sales_tax_rate),
    STRUCT(32 AS market_id, 'Tucson' AS city, 'AZ' AS state_code, 'Arizona' AS state_name, 'West' AS region, 0.60 AS demand_weight, 0.0870 AS sales_tax_rate),
    STRUCT(33 AS market_id, 'Fresno' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 0.70 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(34 AS market_id, 'Sacramento' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 1.10 AS demand_weight, 0.0875 AS sales_tax_rate),
    STRUCT(35 AS market_id, 'Kansas City' AS city, 'MO' AS state_code, 'Missouri' AS state_name, 'Midwest' AS region, 1.00 AS demand_weight, 0.0860 AS sales_tax_rate),
    STRUCT(36 AS market_id, 'Mesa' AS city, 'AZ' AS state_code, 'Arizona' AS state_name, 'West' AS region, 0.60 AS demand_weight, 0.0830 AS sales_tax_rate),
    STRUCT(37 AS market_id, 'Atlanta' AS city, 'GA' AS state_code, 'Georgia' AS state_name, 'South' AS region, 2.50 AS demand_weight, 0.0890 AS sales_tax_rate),
    STRUCT(38 AS market_id, 'Omaha' AS city, 'NE' AS state_code, 'Nebraska' AS state_name, 'Midwest' AS region, 0.70 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(39 AS market_id, 'Raleigh' AS city, 'NC' AS state_code, 'North Carolina' AS state_name, 'South' AS region, 1.40 AS demand_weight, 0.0725 AS sales_tax_rate),
    STRUCT(40 AS market_id, 'Miami' AS city, 'FL' AS state_code, 'Florida' AS state_name, 'South' AS region, 2.40 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(41 AS market_id, 'Virginia Beach' AS city, 'VA' AS state_code, 'Virginia' AS state_name, 'South' AS region, 0.80 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(42 AS market_id, 'Oakland' AS city, 'CA' AS state_code, 'California' AS state_name, 'West' AS region, 0.90 AS demand_weight, 0.1025 AS sales_tax_rate),
    STRUCT(43 AS market_id, 'Minneapolis' AS city, 'MN' AS state_code, 'Minnesota' AS state_name, 'Midwest' AS region, 1.60 AS demand_weight, 0.0888 AS sales_tax_rate),
    STRUCT(44 AS market_id, 'Tulsa' AS city, 'OK' AS state_code, 'Oklahoma' AS state_name, 'South' AS region, 0.60 AS demand_weight, 0.0852 AS sales_tax_rate),
    STRUCT(45 AS market_id, 'Arlington' AS city, 'TX' AS state_code, 'Texas' AS state_name, 'South' AS region, 0.80 AS demand_weight, 0.0825 AS sales_tax_rate),
    STRUCT(46 AS market_id, 'Tampa' AS city, 'FL' AS state_code, 'Florida' AS state_name, 'South' AS region, 1.50 AS demand_weight, 0.0750 AS sales_tax_rate),
    STRUCT(47 AS market_id, 'New Orleans' AS city, 'LA' AS state_code, 'Louisiana' AS state_name, 'South' AS region, 0.70 AS demand_weight, 0.0945 AS sales_tax_rate),
    STRUCT(48 AS market_id, 'Cleveland' AS city, 'OH' AS state_code, 'Ohio' AS state_name, 'Midwest' AS region, 0.80 AS demand_weight, 0.0800 AS sales_tax_rate),
    STRUCT(49 AS market_id, 'Honolulu' AS city, 'HI' AS state_code, 'Hawaii' AS state_name, 'West' AS region, 0.40 AS demand_weight, 0.0471 AS sales_tax_rate),
    STRUCT(50 AS market_id, 'Newark' AS city, 'NJ' AS state_code, 'New Jersey' AS state_name, 'Northeast' AS region, 1.00 AS demand_weight, 0.0663 AS sales_tax_rate),
    STRUCT(51 AS market_id, 'Boise' AS city, 'ID' AS state_code, 'Idaho' AS state_name, 'West' AS region, 0.50 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(52 AS market_id, 'Salt Lake City' AS city, 'UT' AS state_code, 'Utah' AS state_name, 'West' AS region, 0.80 AS demand_weight, 0.0775 AS sales_tax_rate),
    STRUCT(53 AS market_id, 'Burlington' AS city, 'VT' AS state_code, 'Vermont' AS state_name, 'Northeast' AS region, 0.20 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(54 AS market_id, 'Portland' AS city, 'ME' AS state_code, 'Maine' AS state_name, 'Northeast' AS region, 0.20 AS demand_weight, 0.0550 AS sales_tax_rate),
    STRUCT(55 AS market_id, 'Manchester' AS city, 'NH' AS state_code, 'New Hampshire' AS state_name, 'Northeast' AS region, 0.30 AS demand_weight, 0.0000 AS sales_tax_rate),
    STRUCT(56 AS market_id, 'Providence' AS city, 'RI' AS state_code, 'Rhode Island' AS state_name, 'Northeast' AS region, 0.40 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(57 AS market_id, 'Wilmington' AS city, 'DE' AS state_code, 'Delaware' AS state_name, 'South' AS region, 0.30 AS demand_weight, 0.0000 AS sales_tax_rate),
    STRUCT(58 AS market_id, 'Charleston' AS city, 'WV' AS state_code, 'West Virginia' AS state_name, 'South' AS region, 0.20 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(59 AS market_id, 'Charleston' AS city, 'SC' AS state_code, 'South Carolina' AS state_name, 'South' AS region, 0.70 AS demand_weight, 0.0900 AS sales_tax_rate),
    STRUCT(60 AS market_id, 'Sioux Falls' AS city, 'SD' AS state_code, 'South Dakota' AS state_name, 'Midwest' AS region, 0.25 AS demand_weight, 0.0650 AS sales_tax_rate),
    STRUCT(61 AS market_id, 'Fargo' AS city, 'ND' AS state_code, 'North Dakota' AS state_name, 'Midwest' AS region, 0.20 AS demand_weight, 0.0750 AS sales_tax_rate),
    STRUCT(62 AS market_id, 'Billings' AS city, 'MT' AS state_code, 'Montana' AS state_name, 'West' AS region, 0.25 AS demand_weight, 0.0000 AS sales_tax_rate),
    STRUCT(63 AS market_id, 'Cheyenne' AS city, 'WY' AS state_code, 'Wyoming' AS state_name, 'West' AS region, 0.15 AS demand_weight, 0.0600 AS sales_tax_rate),
    STRUCT(64 AS market_id, 'Anchorage' AS city, 'AK' AS state_code, 'Alaska' AS state_name, 'West' AS region, 0.25 AS demand_weight, 0.0000 AS sales_tax_rate),
    STRUCT(65 AS market_id, 'Des Moines' AS city, 'IA' AS state_code, 'Iowa' AS state_name, 'Midwest' AS region, 0.50 AS demand_weight, 0.0700 AS sales_tax_rate),
    STRUCT(66 AS market_id, 'Wichita' AS city, 'KS' AS state_code, 'Kansas' AS state_name, 'Midwest' AS region, 0.60 AS demand_weight, 0.0750 AS sales_tax_rate),
    STRUCT(67 AS market_id, 'Little Rock' AS city, 'AR' AS state_code, 'Arkansas' AS state_name, 'South' AS region, 0.50 AS demand_weight, 0.0925 AS sales_tax_rate),
    STRUCT(68 AS market_id, 'Jackson' AS city, 'MS' AS state_code, 'Mississippi' AS state_name, 'South' AS region, 0.35 AS demand_weight, 0.0800 AS sales_tax_rate),
    STRUCT(69 AS market_id, 'Birmingham' AS city, 'AL' AS state_code, 'Alabama' AS state_name, 'South' AS region, 0.70 AS demand_weight, 0.1000 AS sales_tax_rate),
    STRUCT(70 AS market_id, 'Hartford' AS city, 'CT' AS state_code, 'Connecticut' AS state_name, 'Northeast' AS region, 0.50 AS demand_weight, 0.0635 AS sales_tax_rate)
])
