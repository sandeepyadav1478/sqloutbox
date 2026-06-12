# Contributing to sqloutbox

Thanks for contributing! sqloutbox is a small, dependency-light library; the
bar is high on correctness (it is a durability primitive) and low on
ceremony.

## Development setup

```bash
git clone https://github.com/sandeepyadav1478/sqloutbox
cd sqloutbox
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # installs pytest, pytest-asyncio, ruff, mypy
```

The library itself is **stdlib only** at runtime (plus `tomli` on Python <3.11
for TOML parsing). Do not add runtime dependencies without discussion — the
zero-dependency promise is a feature.

## Quality gates

Every change must pass all three gates before it is merged. Run them locally:

```bash
ruff check src tests          # lint (E, F, I, UP, B rules)
mypy src                      # static types (the package ships py.typed)
python -m pytest -q           # full test suite
```

- **ruff** — linting and import sorting. Fix with `ruff check --fix`.
- **mypy** — static type checking against the public, typed API.
- **pytest** — `asyncio_mode = "auto"` is set in `pyproject.toml`, so
  `@pytest.mark.asyncio` markers are optional (existing tests keep them for
  clarity — match that convention).

## Tests

- Use TDD: write the failing test first, then the implementation.
- Drain/runner tests use a fake `OutboxWriter`. When a test asserts on what was
  delivered, construct the config with `auto_schema=False` AND
  `inject_outbox_seq=False` so the service's startup `_ensure_schema()` /
  `_seed_from_remote()` calls do not pollute the recorded statements.
- Never send a real OS signal in a test (it can interrupt pytest). Capture the
  handler the runner registers and invoke it directly.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `docs:`, `build:`, `test:`, `refactor:`).

## Releasing

1. Update `CHANGELOG.md` with the new version's changes.
2. Bump `version` in `pyproject.toml`.
3. Tag `vX.Y.Z` and push.
