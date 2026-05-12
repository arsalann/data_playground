/* @bruin

name: eu_pfas_staging.pf_source_attribution
type: bq.sql
description: |
  Source-attribution overlay for PFAS sites.

  Two attribution layers:
    1. Site self-classification from the PFAS Data Hub: site_type values such as
       'Military site', 'Firefighting incident / training', 'Airport',
       'Industrial site', 'PFAS production facility', 'Waste management site',
       'Sampling location'. These are direct attributions from the authority
       data the consortium aggregated.
    2. Nearest curated PFAS manufacturer facility (3M / Solvay / Chemours /
       Daikin / Arkema / AGC / Saint-Gobain / Dyneon / Miteni). Banded
       distance: < 5 km, 5-25 km, >= 25 km.

  Per-site attribution_class:
    - 'manufacturer'             -> within 5 km of a curated PFAS manufacturer.
    - 'manufacturer_within_25km' -> 5-25 km of a manufacturer.
    - 'military'                 -> site_type indicates military / firefighting.
    - 'airport'                  -> site_type is 'Airport' and not within 5 km of a manufacturer.
    - 'industrial'               -> site_type is 'Industrial site' or 'PFAS production facility'.
    - 'waste'                    -> site_type is 'Waste management site'.
    - 'unattributed_sample'      -> site_type is 'Sampling location' with no nearby attributable source.
    - 'unattributed'             -> none of the above.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - eu_pfas_staging.pf_sites_dim
  - eu_pfas_raw.manufacturer_facilities

columns:
  - name: site_uid
    type: VARCHAR
    description: PFAS site identifier.
    primary_key: true
  - name: nuts_id
    type: VARCHAR
    description: Assigned NUTS3.
  - name: nearest_manufacturer_km
    type: DOUBLE
    description: Distance to nearest curated PFAS manufacturer facility (km).
  - name: nearest_manufacturer_name
    type: VARCHAR
    description: Manufacturer facility name.
  - name: source_self_class
    type: VARCHAR
    description: PFAS Data Hub site_type as reported.
  - name: attribution_class
    type: VARCHAR
    description: Final per-site attribution label (see description).
  - name: pfas_sum_ng_l
    type: DOUBLE
    description: Concentration (carried through).
  - name: is_water_sample
    type: BOOLEAN
    description: Drinking water / groundwater / surface water flag.

@bruin */

WITH sites AS (
    SELECT
        s.site_uid,
        s.nuts_id,
        s.is_water_sample,
        s.pfas_sum_ng_l,
        s.site_type AS source_self_class,
        s.country_code,
        ST_GEOGPOINT(s.lon, s.lat) AS geo
    FROM `bruin-playground-arsalan.eu_pfas_staging.pf_sites_dim` s
    WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
),

manufacturers AS (
    SELECT
        facility_name,
        company,
        country_code,
        ST_GEOGPOINT(lon, lat) AS geo
    FROM `bruin-playground-arsalan.eu_pfas_raw.manufacturer_facilities`
),

nearest_mfr AS (
    SELECT
        s.site_uid,
        f.facility_name AS nearest_manufacturer_name,
        ST_DISTANCE(s.geo, f.geo) / 1000 AS nearest_manufacturer_km,
        ROW_NUMBER() OVER (PARTITION BY s.site_uid ORDER BY ST_DISTANCE(s.geo, f.geo)) AS rn
    FROM sites s
    CROSS JOIN manufacturers f
),

joined AS (
    SELECT
        s.site_uid,
        s.nuts_id,
        s.is_water_sample,
        s.pfas_sum_ng_l,
        s.source_self_class,
        f.nearest_manufacturer_km,
        f.nearest_manufacturer_name
    FROM sites s
    LEFT JOIN nearest_mfr f ON f.site_uid = s.site_uid AND f.rn = 1
)

SELECT
    site_uid,
    nuts_id,
    nearest_manufacturer_km,
    nearest_manufacturer_name,
    source_self_class,
    CASE
        WHEN nearest_manufacturer_km IS NOT NULL AND nearest_manufacturer_km < 5
            THEN 'manufacturer'
        WHEN source_self_class IN ('Military site', 'Firefighting incident / training')
            THEN 'military'
        WHEN source_self_class = 'PFAS production facility'
            THEN 'manufacturer'
        WHEN source_self_class = 'Industrial site'
            THEN 'industrial'
        WHEN source_self_class = 'Airport'
            THEN 'airport'
        WHEN source_self_class = 'Waste management site'
            THEN 'waste'
        WHEN nearest_manufacturer_km IS NOT NULL AND nearest_manufacturer_km < 25
            THEN 'manufacturer_within_25km'
        WHEN source_self_class = 'Sampling location'
            THEN 'unattributed_sample'
        ELSE 'unattributed'
    END AS attribution_class,
    pfas_sum_ng_l,
    is_water_sample
FROM joined
