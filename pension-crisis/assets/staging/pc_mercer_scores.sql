/* @bruin

name: staging.pc_mercer_scores
type: bq.sql
description: |
  Mercer CFA Institute Global Pension Index 2025 scores for OECD countries, joined
  to the canonical country dimension. Filters to OECD-38 only.
connection: bruin-playground-arsalan

materialization:
  type: table
  strategy: create+replace

depends:
  - raw.pc_mercer_index_2025
  - staging.pc_country_dim

columns:
  - name: iso3_code
    type: VARCHAR
    description: ISO-3 country code.
    primary_key: true
    nullable: false
  - name: country_name
    type: VARCHAR
    description: Country name.
  - name: overall_index
    type: DOUBLE
    description: Mercer GPI overall score (0-100).
  - name: adequacy_sub_index
    type: DOUBLE
    description: Mercer adequacy sub-index (0-100, 40% weight).
  - name: sustainability_sub_index
    type: DOUBLE
    description: Mercer sustainability sub-index (0-100, 35% weight).
  - name: integrity_sub_index
    type: DOUBLE
    description: Mercer integrity sub-index (0-100, 25% weight).
  - name: grade
    type: VARCHAR
    description: Mercer letter grade (A, B+, B, C+, C, D).
  - name: report_edition
    type: VARCHAR
    description: Mercer report edition, e.g. "2025".

@bruin */

WITH mercer_deduped AS (
    SELECT
        iso3_code,
        overall_index,
        adequacy_sub_index,
        sustainability_sub_index,
        integrity_sub_index,
        grade,
        report_edition
    FROM `bruin-playground-arsalan.raw.pc_mercer_index_2025`
    WHERE iso3_code IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY iso3_code ORDER BY extracted_at DESC) = 1
)

SELECT
    d.iso3_code,
    d.country_name,
    m.overall_index,
    m.adequacy_sub_index,
    m.sustainability_sub_index,
    m.integrity_sub_index,
    m.grade,
    m.report_edition
FROM `bruin-playground-arsalan.staging.pc_country_dim` d
LEFT JOIN mercer_deduped m USING (iso3_code)
ORDER BY d.iso3_code
