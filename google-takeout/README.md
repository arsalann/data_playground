# Google Takeout Pipeline

This pipeline analyzes personal Google Takeout data (search history) to measure
changes in search activity before and after the public launch of ChatGPT.

## Data source

- Local HTML export: `data/search-history.html`
- Override path with `GOOGLE_TAKEOUT_SEARCH_HISTORY_PATH`

## Cleaning and processing steps

1. Parse the HTML with BeautifulSoup and scan `div.content-cell` elements.
2. Keep only entries whose content starts with `Searched for`.
3. Extract the search phrase from the first anchor tag.
4. Extract the timestamp from the last text token in the cell.
5. Normalize whitespace (including non-breaking spaces) before parsing.
6. Parse timestamps with explicit GMT offsets and convert to UTC.
7. Skip entries with missing links, empty phrases, or invalid timestamps.
8. Optionally filter rows by `BRUIN_START_DATE` / `BRUIN_END_DATE`.
9. Add `extracted_at` with the current UTC time.

## Outputs

- `raw.google_search_history` with `search_timestamp`, `search_phrase`, and
  `extracted_at`.
- `staging.searches_daily` with daily aggregates and pre/post ChatGPT flags.
