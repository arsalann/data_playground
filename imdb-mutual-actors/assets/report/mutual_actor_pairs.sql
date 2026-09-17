/* @bruin

name: imdb_mutual_report.mutual_actor_pairs
type: bq.sql
description: |
  Title pairs ranked by number of shared billed actors, enriched with both
  titles' metadata and a `likely_related` flag that marks sequels / prequels /
  remakes / spin-offs / same-franchise entries so they can be excluded from the
  headline "unrelated titles that share a cast".

  A pair is flagged likely_related when ANY rule fires:
    1. Curated franchise: both titles map to the same hand-maintained franchise
       label. Covers shared-universe / title-varying franchises that text rules
       miss (Marvel, DC, Star Wars, X-Men, Middle-earth, Wizarding World, Fast &
       Furious, James Bond, American Pie, Meet-the-Parents, Firefly/Serenity,
       Grindhouse, Animaniacs, Despicable Me, Alice in Wonderland, Mexico
       Trilogy, Freaky Friday, Knocked Up, Halloween, Hotel Transylvania, the
       G.O.R.A. universe, Beatles films, John Wick and Jurassic films).
    2. Rare shared title token: the two titles share a normalised word that
       occurs in <= 6 titles across the whole universe (e.g. "shrek", "ralph",
       "blackadder", "spongebob"). Recurring distinctive words are strong
       franchise signals.
    3. Identical normalised base title (subtitle after ':' removed; punctuation,
       articles, roman numerals and sequel numbers stripped) - e.g. "Avatar" /
       "Avatar: The Way of Water", "Mission: Impossible - Fallout".
    4. Shared two-token title prefix - "The Hunger Games ...", "John Wick ...".
    5. One normalised base title contains the other.
    6. Title-token Jaccard similarity >= 0.5.

  Franchise exclusion is curated + heuristic and is not guaranteed exhaustive;
  see the dashboard footnotes for caveats. `both_animation` flags pairs where
  both titles are animated, whose overlap is usually driven by the small pool of
  prolific voice actors rather than a shared production.

materialization:
  type: table
  strategy: create+replace

depends:
  - imdb_mutual_staging.title_pairs_shared_cast
  - imdb_mutual_raw.imdb_titles

secrets:
  - key: bruin-playground-arsalan
    inject_as: bruin-playground-arsalan

columns:
  - name: tconst_a
    type: STRING
    description: First title id
  - name: title_a
    type: STRING
    description: First title display name
  - name: type_a
    type: STRING
    description: First title type
  - name: year_a
    type: INTEGER
    description: First title year
  - name: tconst_b
    type: STRING
    description: Second title id
  - name: title_b
    type: STRING
    description: Second title display name
  - name: type_b
    type: STRING
    description: Second title type
  - name: year_b
    type: INTEGER
    description: Second title year
  - name: shared_actor_count
    type: INTEGER
    description: Distinct billed actors common to both titles
  - name: shared_actors
    type: STRING
    description: Names of the shared actors
  - name: pair_kind
    type: STRING
    description: movie-movie, tv-tv or movie-tv (cross-medium)
  - name: both_animation
    type: BOOLEAN
    description: TRUE when both titles are animated (voice-cast overlap)
  - name: title_jaccard
    type: FLOAT64
    description: Token-set similarity of the two titles (0-1)
  - name: same_franchise
    type: STRING
    description: Curated franchise label when both titles belong to one, else null
  - name: likely_related
    type: BOOLEAN
    description: TRUE when the pair is a sequel/prequel/remake/spin-off/same-franchise

@bruin */

WITH titles AS (
  SELECT
    tconst,
    primary_title,
    title_type,
    start_year,
    num_votes,
    genres,
    LOWER(primary_title) AS lt,
    (genres IS NOT NULL AND REGEXP_CONTAINS(LOWER(genres), r'animation')) AS is_anim
  FROM `bruin-playground-arsalan.imdb_mutual_raw.imdb_titles`
),

franchise AS (
  SELECT
    tconst,
    CASE
      WHEN REGEXP_CONTAINS(lt, r'\b(avengers|iron man|captain america|thor|ant-man|guardians of the galaxy|doctor strange|black panther|captain marvel|black widow|eternals|shang-chi|spider-man|the marvels)\b') THEN 'Marvel (MCU)'
      WHEN REGEXP_CONTAINS(lt, r'\b(batman|superman|justice league|wonder woman|aquaman|suicide squad|shazam|man of steel|birds of prey|blue beetle|the flash)\b') OR lt = 'joker' THEN 'DC'
      WHEN REGEXP_CONTAINS(lt, r'\b(star wars|rogue one)\b') OR lt LIKE 'solo%star wars%' THEN 'Star Wars'
      WHEN REGEXP_CONTAINS(lt, r'\b(x-men|wolverine|deadpool)\b') OR lt = 'logan' THEN 'X-Men'
      WHEN REGEXP_CONTAINS(lt, r'\b(lord of the rings|the hobbit)\b') THEN 'Middle-earth'
      WHEN REGEXP_CONTAINS(lt, r'\b(harry potter|fantastic beasts)\b') THEN 'Wizarding World'
      WHEN REGEXP_CONTAINS(lt, r'furious|fast five|fast x|fast saga|hobbs') OR lt LIKE 'f9%' THEN 'Fast & Furious'
      WHEN REGEXP_CONTAINS(lt, r'^(dr\. no|from russia with love|goldfinger|thunderball|you only live twice|on her majesty.s secret service|diamonds are forever|live and let die|the man with the golden gun|the spy who loved me|moonraker|for your eyes only|octopussy|a view to a kill|the living daylights|licence to kill|goldeneye|tomorrow never dies|the world is not enough|die another day|casino royale|quantum of solace|skyfall|spectre|no time to die)$') THEN 'James Bond'
      WHEN REGEXP_CONTAINS(lt, r'american (pie|wedding|reunion)') THEN 'American Pie'
      WHEN REGEXP_CONTAINS(lt, r'meet the (parents|fockers)|little fockers') THEN 'Meet the Parents'
      WHEN lt IN ('firefly', 'serenity') THEN 'Firefly'
      WHEN REGEXP_CONTAINS(lt, r'grindhouse|death proof|planet terror') THEN 'Grindhouse'
      WHEN REGEXP_CONTAINS(lt, r'animaniacs|pinky and the brain') THEN 'Animaniacs'
      WHEN REGEXP_CONTAINS(lt, r'despicable me') OR lt LIKE 'minions%' THEN 'Despicable Me'
      WHEN REGEXP_CONTAINS(lt, r'^alice (in wonderland|through the looking)') THEN 'Alice in Wonderland'
      WHEN lt IN ('el mariachi', 'desperado', 'once upon a time in mexico') THEN 'Mexico Trilogy'
      WHEN REGEXP_CONTAINS(lt, r'^freak.* friday$') THEN 'Freaky Friday'
      WHEN lt IN ('knocked up', 'this is 40') THEN 'Knocked Up'
      WHEN REGEXP_CONTAINS(lt, r'^halloween\b') THEN 'Halloween'
      WHEN REGEXP_CONTAINS(lt, r'^hotel transylvania\b') THEN 'Hotel Transylvania'
      WHEN lt IN ('g.o.r.a.', 'g.o.r.a', 'a.r.o.g', 'arif v 216') THEN 'G.O.R.A. universe'
      WHEN lt IN ("a hard day's night", 'yellow submarine', 'let it be') THEN 'Beatles films'
      WHEN REGEXP_CONTAINS(lt, r'\b(john wick|ballerina)\b') THEN 'John Wick'
      WHEN REGEXP_CONTAINS(lt, r'\b(jurassic park|jurassic world)\b') THEN 'Jurassic films'
      WHEN lt IN ('jean de florette', 'manon of the spring') THEN 'Jean de Florette'
      ELSE NULL
    END AS franchise
  FROM titles
),

norm AS (
  SELECT
    tconst,
    -- tokens from the FULL title (used for jaccard + rare-token)
    full_toks,
    -- tokens from the part BEFORE ':' (used for base-equality + prefix rules)
    base_toks,
    ARRAY_TO_STRING(base_toks, ' ') AS base,
    ARRAY_TO_STRING(ARRAY(SELECT t FROM UNNEST(base_toks) t WITH OFFSET o WHERE o < 2), ' ') AS prefix2,
    ARRAY_LENGTH(base_toks) AS ntok
  FROM (
    SELECT
      tconst,
      ARRAY(
        SELECT tok FROM UNNEST(SPLIT(TRIM(REGEXP_REPLACE(LOWER(primary_title), r'[^a-z0-9]+', ' ')), ' ')) tok
        WHERE tok != '' AND NOT REGEXP_CONTAINS(tok, r'^[0-9]+$')
          AND tok NOT IN ('the','a','an','of','and','to','in','on','at','for','with','from','part','chapter','vol','volume','episode',
                          'ii','iii','iv','v','vi','vii','viii','ix','x')
      ) AS full_toks,
      -- base tokens keep digits (so "300" / "300: Rise of an Empire" match on base)
      ARRAY(
        SELECT tok FROM UNNEST(SPLIT(TRIM(REGEXP_REPLACE(LOWER(SPLIT(primary_title, ':')[OFFSET(0)]), r'[^a-z0-9]+', ' ')), ' ')) tok
        WHERE tok != ''
          AND tok NOT IN ('the','a','an','of','and','to','in','on','at','for','with','from','part','chapter','vol','volume','episode',
                          'ii','iii','iv','v','vi','vii','viii','ix','x')
      ) AS base_toks
    FROM titles
  )
),

tok_df AS (
  SELECT tok, COUNT(DISTINCT tconst) AS df
  FROM norm, UNNEST(full_toks) tok
  WHERE LENGTH(tok) >= 3
  GROUP BY tok
),

rare_tokens AS (
  SELECT ARRAY_AGG(tok) AS toks FROM tok_df WHERE df <= 6
),

pairs AS (
  SELECT
    p.tconst_a, p.tconst_b, p.shared_actor_count, p.shared_actors,
    ta.primary_title AS title_a, ta.title_type AS type_a, ta.start_year AS year_a, ta.is_anim AS anim_a,
    tb.primary_title AS title_b, tb.title_type AS type_b, tb.start_year AS year_b, tb.is_anim AS anim_b,
    fa.franchise AS fr_a, fb.franchise AS fr_b,
    na.base AS base_a, nb.base AS base_b,
    na.prefix2 AS prefix2_a, nb.prefix2 AS prefix2_b,
    na.ntok AS ntok_a, nb.ntok AS ntok_b,
    na.full_toks AS toks_a, nb.full_toks AS toks_b
  FROM `bruin-playground-arsalan.imdb_mutual_staging.title_pairs_shared_cast` p
  JOIN titles ta ON p.tconst_a = ta.tconst
  JOIN titles tb ON p.tconst_b = tb.tconst
  JOIN franchise fa ON p.tconst_a = fa.tconst
  JOIN franchise fb ON p.tconst_b = fb.tconst
  JOIN norm na ON p.tconst_a = na.tconst
  JOIN norm nb ON p.tconst_b = nb.tconst
),

scored AS (
  SELECT
    *,
    SAFE_DIVIDE(
      (SELECT COUNT(DISTINCT t) FROM UNNEST(toks_a) t WHERE t IN UNNEST(toks_b)),
      (SELECT COUNT(DISTINCT t) FROM UNNEST(ARRAY_CONCAT(toks_a, toks_b)) t)
    ) AS title_jaccard,
    (fr_a IS NOT NULL AND fr_a = fr_b) AS same_franchise_flag,
    EXISTS(
      SELECT 1 FROM UNNEST(toks_a) t
      WHERE t IN UNNEST(toks_b) AND t IN UNNEST((SELECT toks FROM rare_tokens))
    ) AS shared_rare_token
  FROM pairs
)

SELECT
  tconst_a, title_a, type_a, year_a,
  tconst_b, title_b, type_b, year_b,
  shared_actor_count,
  shared_actors,
  CASE
    WHEN type_a = 'movie' AND type_b = 'movie' THEN 'movie-movie'
    WHEN type_a != 'movie' AND type_b != 'movie' THEN 'tv-tv'
    ELSE 'movie-tv'
  END AS pair_kind,
  (anim_a AND anim_b) AS both_animation,
  ROUND(COALESCE(title_jaccard, 0), 3) AS title_jaccard,
  IF(same_franchise_flag, fr_a, NULL) AS same_franchise,
  (
    same_franchise_flag
    OR shared_rare_token
    OR (base_a != '' AND base_a = base_b)
    OR (ntok_a >= 2 AND ntok_b >= 2 AND prefix2_a = prefix2_b)
    OR (LENGTH(base_a) >= 4 AND LENGTH(base_b) >= 4 AND (STRPOS(base_a, base_b) > 0 OR STRPOS(base_b, base_a) > 0))
    OR COALESCE(title_jaccard, 0) >= 0.5
  ) AS likely_related
FROM scored
