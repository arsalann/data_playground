# imdb-mutual-actors

Finds pairs of movies and TV series that share the most **billed actors**, after
excluding sequels, prequels, remakes, spin-offs and same-franchise entries. The
question: which *genuinely distinct* works share a cast?

## Data source

[IMDb Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/)
(`datasets.imdbws.com`). IMDb offers no free public REST API for bulk cast data;
these official TSV exports are the correct source. Personal / non-commercial use
only. Not affiliated with or endorsed by IMDb.

Files used: `title.ratings`, `title.basics`, `title.principals`, `name.basics`.

## Universe

Movies, TV series and TV miniseries, non-adult, with at least **25,000 IMDb
votes**: 8,615 titles, 37,546 billed actors. The vote gate keeps the analysis to
widely recognised titles so overlaps are meaningful rather than obscure-credit
noise.

## Pipeline

```
assets/
  raw/
    imdb_titles.py   -> imdb_mutual_raw.imdb_titles   (ratings + basics, filtered)
    imdb_cast.py     -> imdb_mutual_raw.imdb_cast      (actor/actress credits + names)
  staging/
    title_cast.sql               -> imdb_mutual_staging.title_cast
    title_pairs_shared_cast.sql  -> imdb_mutual_staging.title_pairs_shared_cast
  report/
    mutual_actor_pairs.sql       -> imdb_mutual_report.mutual_actor_pairs
dashboard-dac/                   Bruin DAC dashboard + ibm-cb-dark theme
```

The two raw Python assets stream the gzipped IMDb TSVs and keep only the popular
title set and its acting credits, so nothing near the full 90M-row principals
file is loaded into BigQuery. `imdb_cast` also streams `name.basics` in the same
pass to resolve person ids to display names.

`title_pairs_shared_cast` self-joins the cast on person id (`tconst_a <
tconst_b`) to produce every unordered title pair sharing at least two billed
actors. `mutual_actor_pairs` enriches those pairs and adds the
`likely_related` flag used to exclude directly-related titles.

## "Directly related" exclusion

A pair is flagged `likely_related` (and dropped from the headline) when any rule
fires:

1. **Curated franchise** - both titles map to the same hand-maintained label
   (Marvel, DC, Star Wars, X-Men, Middle-earth, Wizarding World, Fast & Furious,
   James Bond, American Pie, Meet the Parents, Firefly, Grindhouse, Animaniacs,
   Despicable Me, Alice in Wonderland, Mexico Trilogy, Freaky Friday, Knocked
   Up, Halloween, Hotel Transylvania, the G.O.R.A. universe, Beatles films,
   John Wick, Jurassic films and Jean de Florette). Covers shared-universe /
   title-varying franchises that text rules miss.
2. **Rare shared title word** - the two titles share a normalised word occurring
   in six or fewer titles (e.g. "shrek", "ralph", "blackadder").
3. **Identical normalised base title** (subtitle after `:` removed; articles,
   roman numerals and sequel numbers stripped).
4. **Shared two-word title prefix.**
5. **One base title contains the other.**
6. **Title-token Jaccard similarity >= 0.5.**

1,469 pairs are excluded this way; 5,744 remain. This is curated plus heuristic
and **not guaranteed exhaustive**: same-universe films with dissimilar titles can
slip through, and unrelated works with coincidentally similar titles can be
over-excluded.

## Key limitations

- `title.principals` lists only the **top-billed** cast per title (usually up to
  10), so shared-cast counts measure overlap of *leading* casts and understate
  total shared appearances.
- **Animation** pairs (flagged `both_animation`) mostly reflect the small pool of
  prolific voice actors rather than a shared production.
- The **Actor duos** view removes any title appearing in a pair flagged
  `likely_related`, including curated families such as Harry Potter and Star Wars;
  this is conservative and can remove standalone titles caught by a heuristic.

The dashboard's headline "Top Hollywood movie pairs" is further scoped to IMDb
`title_type = movie`, excluding `Documentary` and `Short` genres plus obvious
companion / spin-off-style title patterns. IMDb's current basics export does not
include production country or studio, so "Hollywood" is a transparent
feature-film proxy rather than a verified geography classification.

## Run

```bash
# ingestion (Python assets need Docker running)
bruin run imdb-mutual-actors/assets/raw/imdb_titles.py
bruin run imdb-mutual-actors/assets/raw/imdb_cast.py
# transforms
bruin run imdb-mutual-actors/assets/staging/title_cast.sql
bruin run imdb-mutual-actors/assets/staging/title_pairs_shared_cast.sql
bruin run imdb-mutual-actors/assets/report/mutual_actor_pairs.sql

# dashboard (colour-blind-safe IBM theme)
cd imdb-mutual-actors/dashboard-dac
dac check --dir .
dac serve --dir . --template dashboards/themes/ibm-cb-dark.yml --port 8321
# -> http://localhost:8321
```
