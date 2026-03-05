/* @bruin
name: stock_market.financials_quarterly
type: bq.sql
connection: bruin-playground-arsalan
description: |
  Joins quarterly income statements, balance sheets, and cash flow statements
  into a single analysis-ready table. Adds derived financial ratios including
  margins, returns, leverage, and growth metrics. Enriched with sector/industry.

depends:
  - stock_market_raw.income_statements
  - stock_market_raw.balance_sheets
  - stock_market_raw.cash_flows
  - stock_market_raw.tickers

materialization:
  type: table
  strategy: create+replace

columns:
  - name: ticker
    type: VARCHAR
    description: Stock ticker symbol
    primary_key: true
    nullable: false
  - name: period_ending
    type: DATE
    description: Fiscal quarter end date
    primary_key: true
    nullable: false
  - name: fiscal_year
    type: INTEGER
    description: Fiscal year
  - name: fiscal_quarter
    type: INTEGER
    description: Fiscal quarter (1-4)
  - name: company_name
    type: VARCHAR
    description: Company name
  - name: sector
    type: VARCHAR
    description: GICS sector classification
  - name: sub_industry
    type: VARCHAR
    description: GICS sub-industry classification
  - name: total_revenue
    type: DOUBLE
    description: Total revenue in USD
  - name: cost_of_revenue
    type: DOUBLE
    description: Cost of revenue in USD
  - name: gross_profit
    type: DOUBLE
    description: Gross profit in USD
  - name: operating_expense
    type: DOUBLE
    description: Operating expenses in USD
  - name: operating_income
    type: DOUBLE
    description: Operating income in USD
  - name: net_income
    type: DOUBLE
    description: Net income in USD
  - name: basic_eps
    type: DOUBLE
    description: Basic earnings per share in USD
  - name: diluted_eps
    type: DOUBLE
    description: Diluted earnings per share in USD
  - name: ebitda
    type: DOUBLE
    description: EBITDA in USD
  - name: total_assets
    type: DOUBLE
    description: Total assets in USD
  - name: total_liabilities
    type: DOUBLE
    description: Total liabilities in USD
  - name: stockholders_equity
    type: DOUBLE
    description: Stockholders equity in USD
  - name: retained_earnings
    type: DOUBLE
    description: Retained earnings in USD
  - name: cash_and_equivalents
    type: DOUBLE
    description: Cash and cash equivalents in USD
  - name: current_debt
    type: DOUBLE
    description: Short-term debt in USD
  - name: long_term_debt
    type: DOUBLE
    description: Long-term debt in USD
  - name: total_debt
    type: DOUBLE
    description: Total debt in USD
  - name: net_debt
    type: DOUBLE
    description: Net debt in USD
  - name: shares_outstanding
    type: DOUBLE
    description: Ordinary shares outstanding
  - name: book_value_per_share
    type: DOUBLE
    description: Book value per share (equity / shares outstanding)
  - name: operating_cash_flow
    type: DOUBLE
    description: Operating cash flow in USD
  - name: capital_expenditure
    type: DOUBLE
    description: Capital expenditure in USD
  - name: free_cash_flow
    type: DOUBLE
    description: Free cash flow in USD
  - name: gross_margin_pct
    type: DOUBLE
    description: Gross profit margin percentage
  - name: operating_margin_pct
    type: DOUBLE
    description: Operating income margin percentage
  - name: net_margin_pct
    type: DOUBLE
    description: Net income margin percentage
  - name: roe_pct
    type: DOUBLE
    description: Return on equity percentage (annualized from quarterly)
  - name: roa_pct
    type: DOUBLE
    description: Return on assets percentage (annualized from quarterly)
  - name: debt_to_equity
    type: DOUBLE
    description: Total debt to stockholders equity ratio
  - name: current_ratio
    type: DOUBLE
    description: Current assets to current liabilities ratio
  - name: revenue_qoq_pct
    type: DOUBLE
    description: Revenue quarter-over-quarter growth percentage
  - name: revenue_yoy_pct
    type: DOUBLE
    description: Revenue year-over-year growth percentage
  - name: eps_qoq_pct
    type: DOUBLE
    description: Diluted EPS quarter-over-quarter growth percentage

@bruin */

WITH income AS (
    SELECT *
    FROM stock_market_raw.income_statements
    WHERE period_ending IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, period_ending ORDER BY extracted_at DESC) = 1
),

balance AS (
    SELECT *
    FROM stock_market_raw.balance_sheets
    WHERE period_ending IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, period_ending ORDER BY extracted_at DESC) = 1
),

cashflow AS (
    SELECT *
    FROM stock_market_raw.cash_flows
    WHERE period_ending IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, period_ending ORDER BY extracted_at DESC) = 1
),

joined AS (
    SELECT
        i.ticker,
        i.period_ending,
        i.fiscal_year,
        i.fiscal_quarter,

        t.company_name,
        t.sector,
        t.sub_industry,

        -- Income statement
        COALESCE(i.total_revenue, 0) AS total_revenue,
        i.cost_of_revenue,
        i.gross_profit,
        i.operating_expense,
        i.operating_income,
        i.net_income,
        i.basic_eps,
        i.diluted_eps,
        i.ebitda,
        i.interest_expense,
        i.tax_provision,
        i.research_and_development,
        i.selling_general_and_administration,
        i.diluted_average_shares,

        -- Balance sheet
        b.total_assets,
        b.total_liabilities_net_minority_interest AS total_liabilities,
        b.stockholders_equity,
        b.retained_earnings,
        b.cash_and_cash_equivalents AS cash_and_equivalents,
        b.current_assets,
        b.current_liabilities,
        b.current_debt,
        b.long_term_debt,
        b.total_debt,
        b.net_debt,
        b.goodwill,
        b.net_tangible_assets,
        b.inventory,
        b.accounts_receivable,
        b.accounts_payable,
        b.working_capital,
        b.ordinary_shares_number AS shares_outstanding,

        -- Cash flow
        cf.operating_cash_flow,
        cf.capital_expenditure,
        cf.free_cash_flow,
        cf.investing_cash_flow,
        cf.financing_cash_flow,
        cf.depreciation_and_amortization,
        cf.stock_based_compensation,
        cf.change_in_working_capital,
        cf.common_stock_dividend_paid AS dividends_paid,
        cf.repurchase_of_capital_stock AS share_repurchases

    FROM income i
    LEFT JOIN balance b ON i.ticker = b.ticker AND i.period_ending = b.period_ending
    LEFT JOIN cashflow cf ON i.ticker = cf.ticker AND i.period_ending = cf.period_ending
    LEFT JOIN stock_market_raw.tickers t ON i.ticker = t.ticker
)

SELECT
    ticker,
    period_ending,
    fiscal_year,
    fiscal_quarter,
    company_name,
    sector,
    sub_industry,

    -- Income statement fields
    total_revenue,
    cost_of_revenue,
    gross_profit,
    operating_expense,
    operating_income,
    net_income,
    basic_eps,
    diluted_eps,
    ebitda,
    interest_expense,
    tax_provision,
    research_and_development,
    selling_general_and_administration,
    diluted_average_shares,

    -- Balance sheet fields
    total_assets,
    total_liabilities,
    stockholders_equity,
    retained_earnings,
    cash_and_equivalents,
    current_assets,
    current_liabilities,
    current_debt,
    long_term_debt,
    total_debt,
    net_debt,
    goodwill,
    net_tangible_assets,
    inventory,
    accounts_receivable,
    accounts_payable,
    working_capital,
    shares_outstanding,
    ROUND(stockholders_equity / NULLIF(shares_outstanding, 0), 4) AS book_value_per_share,

    -- Cash flow fields
    operating_cash_flow,
    capital_expenditure,
    free_cash_flow,
    investing_cash_flow,
    financing_cash_flow,
    depreciation_and_amortization,
    stock_based_compensation,
    change_in_working_capital,
    dividends_paid,
    share_repurchases,

    -- Derived margins
    ROUND(gross_profit / NULLIF(total_revenue, 0) * 100, 2) AS gross_margin_pct,
    ROUND(operating_income / NULLIF(total_revenue, 0) * 100, 2) AS operating_margin_pct,
    ROUND(net_income / NULLIF(total_revenue, 0) * 100, 2) AS net_margin_pct,

    -- Derived returns (annualized: quarterly net income × 4)
    ROUND(net_income * 4 / NULLIF(stockholders_equity, 0) * 100, 2) AS roe_pct,
    ROUND(net_income * 4 / NULLIF(total_assets, 0) * 100, 2) AS roa_pct,

    -- Derived leverage
    ROUND(total_debt / NULLIF(stockholders_equity, 0), 4) AS debt_to_equity,
    ROUND(current_assets / NULLIF(current_liabilities, 0), 4) AS current_ratio,

    -- Growth metrics
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY ticker ORDER BY period_ending))
        / NULLIF(ABS(LAG(total_revenue) OVER (PARTITION BY ticker ORDER BY period_ending)), 0) * 100,
        2
    ) AS revenue_qoq_pct,

    ROUND(
        (total_revenue - LAG(total_revenue, 4) OVER (PARTITION BY ticker ORDER BY period_ending))
        / NULLIF(ABS(LAG(total_revenue, 4) OVER (PARTITION BY ticker ORDER BY period_ending)), 0) * 100,
        2
    ) AS revenue_yoy_pct,

    ROUND(
        (diluted_eps - LAG(diluted_eps) OVER (PARTITION BY ticker ORDER BY period_ending))
        / NULLIF(ABS(LAG(diluted_eps) OVER (PARTITION BY ticker ORDER BY period_ending)), 0) * 100,
        2
    ) AS eps_qoq_pct

FROM joined
ORDER BY ticker, period_ending
