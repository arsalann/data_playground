/* @bruin
name: bruin_shop_raw.shopify_customers
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Deterministic fake Shopify-style customer profiles for a US-only apparel
  company. Customer pools are generated per city market so every order can be
  tied to a stable customer, state, and city without relying on live Shopify.

depends:
  - bruin_shop_raw.us_markets
  - bruin_shop_raw.shopify_customers_test

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: VARCHAR
    description: Synthetic Shopify customer identifier.
    primary_key: true
    nullable: false
  - name: email
    type: VARCHAR
    description: Customer email address.
  - name: first_name
    type: VARCHAR
    description: Customer first name.
  - name: last_name
    type: VARCHAR
    description: Customer last name.
  - name: created_at
    type: TIMESTAMP
    description: Customer profile creation timestamp in UTC.
  - name: updated_at
    type: TIMESTAMP
    description: Customer profile update timestamp in UTC.
  - name: orders_count
    type: INTEGER
    description: Placeholder lifetime order count; staging recomputes exact counts from synthetic orders.
  - name: total_spent
    type: DOUBLE
    description: Placeholder lifetime spend in USD; staging recomputes exact spend from synthetic orders.
  - name: tags
    type: VARCHAR
    description: Shopify-style customer tags.
  - name: state
    type: VARCHAR
    description: Shopify customer account state.
  - name: city
    type: VARCHAR
    description: Customer city.
  - name: state_code
    type: VARCHAR
    description: Two-letter US state or district postal abbreviation.
  - name: state_name
    type: VARCHAR
    description: Full US state or district name.
  - name: zip
    type: VARCHAR
    description: Synthetic ZIP code.

@bruin */

WITH first_names AS (
    SELECT name, offset + 1 AS name_id
    FROM UNNEST(['Avery', 'Maya', 'Noah', 'Sofia', 'Ethan', 'Lina', 'Marcus', 'Priya', 'Iris', 'Julian', 'Nora', 'Caleb', 'Tara', 'Owen', 'Leah', 'Miles', 'Amara', 'Grace', 'Mateo', 'Jade']) AS name WITH OFFSET
),
last_names AS (
    SELECT name, offset + 1 AS name_id
    FROM UNNEST(['Rivera', 'Chen', 'Patel', 'Morgan', 'Kim', 'Singh', 'Garcia', 'Nguyen', 'Brown', 'Khan', 'Johnson', 'Davis', 'Martinez', 'Wilson', 'Anderson', 'Clark', 'Lewis', 'Walker', 'Young', 'Allen']) AS name WITH OFFSET
),
market_customers AS (
    SELECT
        m.*,
        CAST(1200 + ROUND(m.demand_weight * 2800) AS INT64) AS customer_count
    FROM bruin_shop_raw.us_markets m
),
customers AS (
    SELECT
        m.market_id,
        m.city,
        m.state_code,
        m.state_name,
        customer_index,
        FORMAT('cust_%03d_%06d', m.market_id, customer_index) AS customer_id,
        DATE_ADD(DATE '2024-01-01', INTERVAL ABS(MOD(FARM_FINGERPRINT(FORMAT('%03d-%06d-created', m.market_id, customer_index)), 515)) DAY) AS created_date,
        ABS(MOD(FARM_FINGERPRINT(FORMAT('%03d-%06d-name', m.market_id, customer_index)), 20)) + 1 AS first_name_id,
        ABS(MOD(FARM_FINGERPRINT(FORMAT('%03d-%06d-last', m.market_id, customer_index)), 20)) + 1 AS last_name_id
    FROM market_customers m
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, m.customer_count)) AS customer_index
)

SELECT
    c.customer_id AS id,
    FORMAT('apparel-%03d-%06d@example.com', c.market_id, c.customer_index) AS email,
    fn.name AS first_name,
    ln.name AS last_name,
    TIMESTAMP(DATETIME(c.created_date, TIME(10, MOD(c.customer_index, 60), 0)), 'UTC') AS created_at,
    TIMESTAMP(DATETIME(DATE '2026-06-05', TIME(12, 0, 0)), 'UTC') AS updated_at,
    0 AS orders_count,
    0.00 AS total_spent,
    CONCAT('synthetic,apparel,', LOWER(c.state_code), IF(MOD(c.customer_index, 17) = 0, ',vip', ''), IF(MOD(c.customer_index, 9) = 0, ',newsletter', '')) AS tags,
    'enabled' AS state,
    c.city,
    c.state_code,
    c.state_name,
    FORMAT('%05d', 10000 + ABS(MOD(FARM_FINGERPRINT(FORMAT('%03d-%06d-zip', c.market_id, c.customer_index)), 89999))) AS zip
FROM customers c
INNER JOIN first_names fn
    ON c.first_name_id = fn.name_id
INNER JOIN last_names ln
    ON c.last_name_id = ln.name_id
ORDER BY c.market_id, c.customer_index
