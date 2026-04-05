# Core Runtime Columnar Plan

This document is the current working plan for the core runtime columnar path.
It is intended to complement older design notes in `plans/` by separating:

- what is already implemented
- what is intentionally not in scope for this line of work
- what the next phases are

## Document Role

Use repository documents with the following split:

- root `README.md` / `README-zh.md`
  - repository-facing implementation summary and current scope
- `docs/core-boundary.md`
  - ownership and layering boundaries
- `docs/runtime-contract.md`
  - stable runtime-facing contract
- `docs/streaming_runtime_design.md`
  - current streaming/runtime implementation shape
- this document
  - current status board for the core-runtime columnar line only

This plan should not become a second copy of the whole repository README.
When repository-wide scope changes, update the root README first and keep this file focused on the columnar runtime track.

## Goal

Keep the existing core kernel contract stable while reducing repeated `Arrow -> row -> column`
and `row -> column` conversions inside the runtime.

Column-first is not only an optimization technique in this plan; it is the intended direction of the core runtime.
Rows remain a compatibility representation for selected boundaries, but they should not be treated as the desired steady-state internal form.

The practical direction is:

- preserve a reusable columnar representation deeper into the core runtime
- let existing operators consume and produce that representation more consistently
- avoid accepting SQL or runtime shapes that do not have a stable executor mapping
- make performance, copy reduction, and late materialization the default decision criteria for hot-path changes

This plan does not introduce a second execution engine and does not redefine the public
`DataflowSession` / `session.sql(...)` / streaming contract.

## Implemented

The following pieces are already in place in the current repository state.

### SQL v1 and streaming boundary alignment

- core SQL v1 batch path is stabilized around:
  - `CREATE TABLE`
  - `CREATE SOURCE TABLE`
  - `CREATE SINK TABLE`
  - `INSERT INTO ... VALUES`
  - `INSERT INTO ... SELECT`
  - `SELECT` with projection/alias, `WHERE`, `GROUP BY`, `LIMIT`, and current minimal `JOIN`
- stream SQL entrypoints are aligned around the existing runtime boundary:
  - `streamSql(...)`
  - `explainStreamSql(...)`
  - `startStreamSql(...)`
- unsupported SQL v1 shapes are rejected explicitly instead of falling through to ambiguous failures
- table-kind constraints are enforced on the SQL path:
  - source tables are read-only
  - sink tables are write-only from the SQL planner perspective

### String builtins and batch-oriented execution

- string builtins are available on the batch SQL path:
  - `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`
  - `LENGTH`, `LEN`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `REVERSE`
  - `CONCAT`, `CONCAT_WS`, `LEFT`, `RIGHT`, `SUBSTR` / `SUBSTRING`, `POSITION`, `REPLACE`
- planner and executor now use a consistent mapping for these builtins
- execution was moved away from per-row ad hoc argument assembly toward shared batch-oriented helpers

### Shared columnar helper layer

- a shared columnar helper layer exists under:
  - `src/dataflow/core/execution/columnar_batch.h`
  - `src/dataflow/core/execution/columnar_batch.cc`
- the helper layer currently covers:
  - value-column materialization
  - string/int column materialization
  - row selection
  - projected/filter/limit table helpers
  - window bucket assignment helper
  - serialized group/join key materialization
  - hash bucket construction
  - shared string kernels
- some helper APIs still materialize full `ValueColumnBuffer` copies today; reducing those copies is part of the next-phase work rather than a completed item

### Retained columnar cache on `Table`

- `Table` now carries a reusable `columnar_cache` sidecar
- cache hydration is lazy when a table enters a column-oriented helper path
- table copy semantics deep-copy the cache instead of aliasing it unsafely
- common table transforms keep or rebuild the cache intentionally rather than dropping it silently
- the repository is in transition away from `Table -> Row -> Value` as the dominant hot-path form
- the retained cache is still an intermediate step, not the final architecture target

### Operators already moved off result-tail full-table rematerialization

- batch runtime:
  - `Select`
  - `Filter`
  - `Drop`
  - `Limit`
  - `WindowAssign`
  - string `WithColumn`
  - `Aggregate`
  - current minimal `Join`
- stream runtime:
  - `select / filter / withColumn / drop / limit / window`
  - stateful grouped aggregate output
  - actor grouped aggregate finalize output
- Arrow/Python boundary:
  - Arrow import fast path
  - Arrow import slow path
  - nanoarrow batch materialization
  - Arrow export schema inference
  - Arrow export array/buffer construction

### Stream local-workers cache retention

- stream partition split/merge now carries columnar cache forward
- `local_workers > 1` no longer forces these paths back into an avoidable row-only rematerialization pattern

### Regression coverage already added

- planner roundtrip coverage for:
  - `WindowAssign`
  - string `WithColumn`
  - `Aggregate`
  - `Join`
- stream runtime coverage for:
  - stateful window/grouped aggregate output
  - local-workers cache retention
- Arrow/Python regression coverage for:
  - fast path import
  - slow path import
  - export roundtrip
- source materialization regression for nanoarrow roundtrip

## Not Planned In This Line

The following items are intentionally not part of the current core-runtime-columnar plan.

### SQL surface expansion beyond current v1 boundary

- `CTE`
- subquery support
- `UNION`
- broader or more complex join semantics beyond the current minimal support

### Hot-path Python extensibility

- Python callbacks in the execution hot path
- Python UDFs executed inside the core runtime hot path

### A second execution engine

- replacing the core runtime with a separate Python-driven execution engine
- redefining explain/progress/checkpoint semantics in the Python layer

### Distributed runtime claims

- claiming production-ready distributed scheduling
- claiming production-grade distributed fault recovery
- turning the current same-host actor/rpc path into a fully completed distributed runtime

### Broad vector/database scope expansion

- ANN as a separate current-track deliverable
- standalone vector database positioning
- distributed vector serving/execution as part of this plan

## Next Phases

The next phases should stay narrow and measurable.

### 1. Promote columnar data from sidecar cache to a stronger internal representation

- keep the public contract unchanged
- reduce dependence on `Table -> Row -> Value` as the dominant internal form
- allow more internal operator chaining without repeated row reconstruction
- make rows a boundary-only compatibility layer wherever practical

### 2. Extend end-to-end columnar operator coverage

Priority operators:

- `select`
- `filter`
- `withColumn`
- `window`
- `aggregate`
- `join`

The goal is not only to consume cached columns at the edges, but to avoid rebuilding rows and then
rematerializing the same columns again in the middle of the pipeline.

### 3. Keep Arrow / nanoarrow data columnar deeper into the runtime

- reduce repeated ingress/egress conversion cost
- let Arrow-aligned data survive longer before falling back to row materialization
- preserve a clearer path to future zero-copy or lower-copy export/import work

### 4. Add explicit performance baselines

Required benchmark targets:

- hardcode vs native kernel batch comparison on shared inputs
- string builtin execution
- stream local-workers execution
- Arrow import/export

The point is to make columnar work measurable and catch regressions before they become semantic-only improvements.

### 5. Continue builtin expansion only on the same stable path

Additional builtin functions are acceptable only when:

- planner mapping is explicit
- executor semantics are stable
- unsupported shapes still fail clearly
- the implementation stays on the same batch-oriented helper path instead of introducing a second ad hoc execution path

## Related Historical Notes

Older plan documents in `plans/` still describe earlier design stages and should be treated as background, not as the current status board. The most directly related historical notes are:

- `plans/stream-sql-v1.md`
- `plans/streaming-first-roadmap.md`
- `plans/python-api-v1.md`
