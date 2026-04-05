# Velaria Core Boundary

## Summary

Velaria is organized around one kernel and two non-kernel layers:

- `Core Kernel`
  - pure `C++20`
  - local-first execution
  - batch and stream share one execution model
  - runtime behavior is exposed through stable contract surfaces
- `Python Ecosystem`
  - supported ingress, interop, packaging, and automation layer
  - Arrow, wheel/CLI, `uv`, Excel/Bitable, custom stream adapters
  - projects core behavior outward, but does not define execution semantics
- `Experimental Runtime`
  - same-host `actor/rpc/jobmaster`
  - observability and execution research lane
  - not a second production kernel

The only golden path is:

```text
Arrow / CSV / Python ingress
  -> DataflowSession / DataFrame / StreamingDataFrame
  -> local runtime kernel
  -> sink
  -> explain / progress / checkpoint
```

## Layering

### Core Kernel

Core owns the semantics that must stay stable across C++, Python, demos, and future integrations:

- logical planning and minimal SQL mapping
- table/value model
- local execution and streaming runtime
- vector search as a local index capability
- `DataflowSession`, `DataFrame`, `StreamingDataFrame`
- source/sink ABI
- progress / checkpoint / explain contract

Repository view:

- Bazel source groups:
  - `//:velaria_core_logical_sources`
  - `//:velaria_core_execution_sources`
  - `//:velaria_core_contract_sources`
- regression entrypoint:
  - `//:core_regression`

### Python Ecosystem

Python is a supported ecosystem layer, not a convenience sidecar.

Python ecosystem owns:

- native binding surface in `python_api`
- supported library modules in `python_api/velaria`
- supported CLI tooling in `python_api/velaria_cli.py`
- Arrow ingestion/output
- `uv`-based development and test workflow
- wheel, native wheel, and CLI packaging
- Excel and Bitable adapters
- custom source / custom sink adapters
- Python-facing demos in `python_api/examples`
- Python-facing benchmarks in `python_api/benchmarks`

Python ecosystem must not:

- redefine runtime contract semantics
- introduce a Python hot path for core execution
- become the source of truth for progress/checkpoint/explain behavior
- require experimental runtime components for normal operation

Repository view:

- Bazel source group:
  - `//:velaria_python_ecosystem_sources`
- Python-layer source groups:
  - `//python_api:velaria_python_supported_sources`
  - `//python_api:velaria_python_example_sources`
  - `//python_api:velaria_python_experimental_sources`
- regression entrypoint:
  - `//:python_ecosystem_regression`
- Python-layer regression entrypoint:
  - `//python_api:velaria_python_supported_regression`
- shell entrypoint:
  - `./scripts/run_python_ecosystem_regression.sh`

### Experimental Runtime

Experimental runtime remains in the repo because it is useful for same-host execution and observability research.

It includes:

- actor runtime
- rpc codec and same-host transport experiments
- scheduler / worker / client flow
- same-host benchmark and smoke scripts

It does not redefine:

- the public session entry
- batch/stream semantics
- checkpoint delivery contract
- vector query semantics

Repository view:

- Bazel source group:
  - `//:velaria_experimental_sources`
- regression entrypoint:
  - `//:experimental_regression`
- shell entrypoint:
  - `./scripts/run_experimental_regression.sh`

### Examples

Examples demonstrate layers; they do not define them.

Examples include:

- single-node demos
- vector benchmarks
- same-host smoke tools
- local helper scripts and skills

Repository view:

- Bazel source group:
  - `//:velaria_examples_sources`

## Ownership Rules

- `DataflowSession` remains the only public session entry.
- SQL stays an ingress surface. It does not back-drive runtime design.
- Python remains supported, but cannot become the execution core.
- Vector search remains a core local capability, not a new subsystem.
- Same-host actor/rpc stays experimental, even when it is featureful.
- `sql_demo / df_demo / stream_demo` are the single-node baseline and must remain intact.

## Full-Reorg Note

This reorg is now implemented in two layers:

- repository-facing structure:
  - layered Bazel source groups
  - layered regression suites
  - layered documentation
  - README and Python ecosystem reordering
- physical source layout:
  - `src/dataflow/core/logical`
  - `src/dataflow/core/execution`
  - `src/dataflow/core/contract`
  - `src/dataflow/interop`
  - `src/dataflow/experimental`

The build graph is still being separated incrementally. Some targets still depend across layers for compatibility while the physical directory split is established and validated.
