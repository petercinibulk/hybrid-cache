# Contributing

Thanks for helping improve `async-hybrid-cache`.

## Set Up

```bash
uv sync --all-groups
```

## Check Your Changes

Run the same checks used by CI:

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest
uv build --no-sources
```

## Versioning and Changelog

This project uses SemVer, Conventional Commits, and Release Please.

Use a Conventional Commit title for every pull request:

```text
fix: correct stale cache invalidation
feat: add memcached provider
docs: document Redis invalidation setup
feat!: change cache key serialization
```

The PR title is checked by CI. Use squash merge so the commit that lands on `main`
keeps that title.

Release Please runs after pushes to `main`. When releasable commits land, it opens
or updates a release PR that bumps the version in `pyproject.toml` and updates
`CHANGELOG.md`.

Merge the release PR when you want to publish the release. Merging it creates the
Git tag and GitHub release.

Version bumps are automatic:

```text
fix:    patch release
feat:   minor release
feat!:  major release
```

## Work on Documentation

The documentation site is built with Zensical.

```bash
uv run zensical serve
uv run zensical build --clean --strict
```

Documentation should stay focused on people using `async-hybrid-cache` in their applications. Follow the Diataxis structure in `docs/`: tutorials, how-to guides, reference, and explanation.

## Pull Requests

Keep pull requests focused on one change. Include tests for behavior changes, update documentation when user-facing behavior changes, and make sure the checks above pass before opening a PR.
