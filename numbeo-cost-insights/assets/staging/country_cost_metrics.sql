/* @bruin
name: numbeo_staging.country_cost_metrics
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deduplicates Numbeo country snapshots and derives affordability-gap metrics.
  The squeeze index compares combined cost plus rent with local purchasing power.
depends:
  - numbeo_raw.numbeo_country_rankings

materialization:
  type: table
  strategy: create+replace

columns:
  - name: country_snapshot_key
    type: VARCHAR
    description: Stable country and snapshot-date key.
    primary_key: true
  - name: country
    type: VARCHAR
    description: Country or territory name.
  - name: continent
    type: VARCHAR
    description: Broad continent grouping assigned from the Numbeo country label for dashboard slicing.
  - name: snapshot_date
    type: DATE
    description: Date of the source snapshot.
  - name: cost_of_living_index
    type: DOUBLE
    description: Consumer cost index relative to New York City = 100.
  - name: rent_index
    type: DOUBLE
    description: Rent index relative to New York City = 100.
  - name: cost_of_living_plus_rent_index
    type: DOUBLE
    description: Combined cost and rent index relative to New York City = 100.
  - name: groceries_index
    type: DOUBLE
    description: Groceries index relative to New York City = 100.
  - name: restaurant_price_index
    type: DOUBLE
    description: Restaurant price index relative to New York City = 100.
  - name: local_purchasing_power_index
    type: DOUBLE
    description: Local purchasing power index relative to New York City = 100.
  - name: affordability_gap_index
    type: DOUBLE
    description: Combined cost plus rent minus local purchasing power; positive means a local affordability squeeze.
  - name: rent_to_purchasing_power_ratio
    type: DOUBLE
    description: Rent index divided by local purchasing power index; ratio of indexed burdens.
  - name: restaurant_minus_grocery_index
    type: DOUBLE
    description: Restaurant price index minus groceries index.
  - name: affordability_class
    type: VARCHAR
    description: Broad classification based on whether combined costs exceed local purchasing power.

@bruin */

WITH deduped AS (
    SELECT *
    FROM numbeo_raw.numbeo_country_rankings
    WHERE country_snapshot_key IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_snapshot_key
        ORDER BY extracted_at DESC
    ) = 1
)

SELECT
    country_snapshot_key,
    country,
    CASE
        WHEN country IN ('Algeria', 'Angola', 'Botswana', 'Cameroon', 'Cape Verde', 'Democratic Republic of the Congo', 'Egypt', 'Ethiopia', 'Ghana', 'Ivory Coast', 'Kenya', 'Libya', 'Madagascar', 'Mauritius', 'Morocco', 'Mozambique', 'Namibia', 'Nigeria', 'Rwanda', 'Senegal', 'Seychelles', 'South Africa', 'Tanzania', 'Tunisia', 'Uganda', 'Zambia', 'Zimbabwe') THEN 'Africa'
        WHEN country IN ('Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Brunei', 'Cambodia', 'China', 'Georgia', 'Hong Kong (China)', 'India', 'Indonesia', 'Iran', 'Iraq', 'Israel', 'Japan', 'Jordan', 'Kazakhstan', 'Kuwait', 'Kyrgyzstan', 'Lebanon', 'Macao (China)', 'Malaysia', 'Maldives', 'Mongolia', 'Myanmar', 'Nepal', 'Oman', 'Pakistan', 'Philippines', 'Qatar', 'Saudi Arabia', 'Singapore', 'South Korea', 'Sri Lanka', 'Syria', 'Taiwan', 'Tajikistan', 'Thailand', 'Turkey', 'Turkmenistan', 'United Arab Emirates', 'Uzbekistan', 'Vietnam', 'Yemen') THEN 'Asia'
        WHEN country IN ('Albania', 'Austria', 'Belarus', 'Belgium', 'Bosnia And Herzegovina', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Gibraltar', 'Greece', 'Guernsey', 'Hungary', 'Iceland', 'Ireland', 'Isle Of Man', 'Italy', 'Jersey', 'Kosovo (Disputed Territory)', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Montenegro', 'Netherlands', 'North Macedonia', 'Norway', 'Poland', 'Portugal', 'Romania', 'Russia', 'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Ukraine', 'United Kingdom') THEN 'Europe'
        WHEN country IN ('Bahamas', 'Belize', 'Bermuda', 'Canada', 'Cayman Islands', 'Costa Rica', 'Cuba', 'Dominican Republic', 'El Salvador', 'Guatemala', 'Honduras', 'Jamaica', 'Mexico', 'Nicaragua', 'Panama', 'Puerto Rico', 'Trinidad And Tobago', 'United States') THEN 'North America'
        WHEN country IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela') THEN 'South America'
        WHEN country IN ('Australia', 'Fiji', 'New Zealand', 'Papua New Guinea', 'Solomon Islands') THEN 'Oceania'
        ELSE 'Other'
    END AS continent,
    snapshot_date,
    cost_of_living_index,
    rent_index,
    cost_of_living_plus_rent_index,
    groceries_index,
    restaurant_price_index,
    local_purchasing_power_index,
    cost_of_living_plus_rent_index - local_purchasing_power_index AS affordability_gap_index,
    SAFE_DIVIDE(rent_index, NULLIF(local_purchasing_power_index, 0)) AS rent_to_purchasing_power_ratio,
    restaurant_price_index - groceries_index AS restaurant_minus_grocery_index,
    CASE
        WHEN cost_of_living_plus_rent_index > local_purchasing_power_index THEN 'squeeze'
        WHEN cost_of_living_plus_rent_index >= local_purchasing_power_index * 0.75 THEN 'tight'
        ELSE 'cushion'
    END AS affordability_class
FROM deduped
