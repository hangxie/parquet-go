# Engineering Skills & Workflow Standards (parquet-go)

This document defines expectations for contributors
to the parquet-go library project.

---

## Project Domain Skills

- Comfortable with Apache Parquet file format, including metadata, schemas, encodings, and compression.
- Experienced with Go library design, including clean public APIs and streaming IO.
- Familiarity with Parquet physical types, logical types (including geospatial, FLOAT16, UUID, VARIANT), and encoding schemes.
- Understanding of storage abstractions for multiple backends (local, S3, GCS, Azure, HDFS, HTTP).

---

## Go Engineering Standards

- Follow idiomatic Go style; run `gofumpt`/`goimports` on changes.
- Handle errors explicitly and return meaningful error messages.
- Use `context.Context` for cancellable operations.
- Avoid leaking goroutines or unnecessary memory allocations.
- Maintain clean public API surfaces; avoid exposing internal details.

---

## Testing & TDD Expectations

- Tests must cover both typical and edge-case scenarios.
- Use **table-driven tests** for core logic (the dominant pattern in this codebase).
- Tests typically use in-memory buffers (`bytes.Buffer`) and inline test data rather than external test files.
- Use **round-trip testing** (write data, read back, compare) for encoding, compression, and reader/writer validation.
- Use `github.com/stretchr/testify/require` for assertions.
- Validate changes with `make all` (format, lint, test, example, build) and maintain/improve coverage.
- Contributors are expected to follow **TDD principles**:
  - Design tests before implementation.
  - Ensure tests fail before implementing the feature.
  - Make small, reviewable commits after completing each logical phase.

---

## Library & Documentation Quality

- Public APIs must have clear GoDoc comments.
- Examples in `example/` should be buildable (tagged with `//go:build example`) and demonstrate real use cases.
- Documentation (`README.md`, `geoparquet.md`, `source/README.md`) must reflect all behavioral changes.
- Compatibility notes should reflect supported platforms and Parquet format versions.

---

## Contribution Workflow Norms

- **Always commit after completing a task.** Once `make all` passes, create a commit immediately — do not wait to be asked.
- Commit messages should follow **Conventional Commits**.
- Avoid breaking backward compatibility without clear migration notes.
- Maintain high-quality, reviewable commits after completing each logical phase (plan, test, implement, document).

---

## Quality Gates

- Test coverage must be maintained or improved on significant logic changes.
- Code must pass formatting, linting, testing, and build checks (`make all`) before merging.
- Refactoring and cleanup must preserve behavior and pass validation.

---

## Task Tracking Process

When working on improvements or fixes from a review:

1. **Prepare `TODO.md`** - Create or update `TODO.md` with numbered, categorized items. Use checkboxes (`- [ ]`) to track completion.
2. **Make changes** - Implement the fix or improvement for the selected item(s).
3. **Validate** - Run `make all` and ensure format, lint, test, and build all pass.
4. **Commit** - Commit only the source code changes. Do **not** commit `TODO.md`.
5. **Mark complete** - Update `TODO.md` to check off the completed item(s) (`- [x]`).
