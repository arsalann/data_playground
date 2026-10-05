# QA checks for `murder-mystery`

## THIS DIRECTORY IS A COMPLETE SPOILER

The query text in `checks/` **is** the deduction path, stage by stage. Reading it will
ruin the game. It lives in `qa/`, outside `assets/`, for exactly that reason: Bruin only
picks up assets under `assets/`, so a normal `bruin run` never touches these.

## What these check

The game ships no verdict mechanism: nothing in a player's project tells them whether
their accusation is right. These checks are therefore the *only* guarantee that the case
has exactly one answer. Without them, an innocuous change to a generator quietly makes
the case unsolvable or trivial and nobody finds out until a player complains.

Each asset materializes one stage's candidate set into a `qa` schema and asserts its
size with a `custom_checks` entry. The last asset asserts that the four candidate sets
converge on the four residents the generator actually chose — which is the part that
proves the stages lead somewhere rather than merely returning the right *number* of rows.

## How they run

They need the generation scaffolding alive, which the pipeline's last asset drops. So the
run excludes it by tag:

```bash
# from the repo root
cp -R murder-mystery/qa/checks murder-mystery/assets/checks
bruin run --full-refresh --exclude-tag scaffolding murder-mystery
rm -rf murder-mystery/assets/checks
```

Any failing `custom_check` fails the run, which is the point. Run this after any change to
`assets/seed/` or `macros/`. Remove the copied checks afterwards — left in `assets/`, they
are a spoiler sitting next to the player's notebook.

Reading `_gen.actor_assignments` on a scaffolding-preserved run is the only sanctioned way
to learn the answer, and it is why the teardown is its own asset rather than a trailing
statement inside another one.
