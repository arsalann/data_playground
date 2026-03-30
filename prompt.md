# Pipeline Prompt Template

Generic template for prompting an agent to build a new data pipeline in this repo.
Copy, fill in the bracketed sections, and remove any optional blocks you don't need.

---

## Prompt

Build a new pipeline similar to @stackoverflow-trends for **[DOMAIN / DATA SOURCE]**.

**Context:** utilize Bruin MCP and Bruin CLI, reference Bruin docs. Follow @AGENTS.md strictly — these are the rules for this repo. If you are about to break any rule in AGENTS.md, stop and ask for clarification and permission before proceeding.

### Data source

[Provide ONE of the following:]

- API documentation link: [URL]
- Relevant endpoints:
  - `[endpoint_1]`
  - `[endpoint_2]`
  - `[endpoint_3]`
- OR: describe the data source if not API-based (e.g. public BigQuery dataset, CSV files, web scraping)

[If credentials are already configured:]
Credentials are in @.bruin.yml ([line range])

### What to extract

[Describe the entities and fields you care about. Be specific about granularity and history depth.]

Examples:

- "daily OHLCV prices + market cap for every S&P 500 ticker, going back to 2012"
- "hourly generation by energy source (MWh), day-ahead forecasts, market clearing prices, for the past 1 year"

### Naming

All asset/table names should have prefix `**[prefix]_`**
Destination: BigQuery

### Dashboard questions

The goal is a final dashboard that answers:

- [Question 1]
- [Question 2]
- [Question 3]
- [Question 4]

### Build order

Start building the pipeline:

1. Create pipeline structure and a README outlining the pipeline, data sources, and processing method
2. Create the Python extraction assets (with Bruin Python materialization), use Bruin's built-in start/end date variables
3. Test each raw asset individually for a small subset of data ([describe subset, e.g. "2-3 days" or "a single quarter"])
4. Build the staging SQL transformation assets, test them
5. Test the entire pipeline end-to-end for a slightly larger window ([describe window, e.g. "1 week"])

### Constraints (optional)

[Any additional constraints, e.g.:]

- "start with a free API for now, we can switch to a paid one later"
- "use append-only strategy and deduplicate in staging"
- "financial statements should fetch all available quarters regardless of date range"

