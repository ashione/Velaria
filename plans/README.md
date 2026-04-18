# Plans Directory Guide

The Chinese mirror lives in [README-zh.md](./README-zh.md).

This directory mixes current plan tracking, implemented-design background, and older roadmap notes.
Use this index to decide which plan document is authoritative for the question you are asking.

## Read Order

- repository-facing implementation and current scope:
  - [../README.md](../README.md)
  - [../README-zh.md](../README-zh.md)
- stable boundaries and contracts:
  - [../docs/core-boundary.md](../docs/core-boundary.md)
  - [../docs/runtime-contract.md](../docs/runtime-contract.md)
  - [../docs/agentic-service-api.md](../docs/agentic-service-api.md)
  - [../docs/streaming_runtime_design.md](../docs/streaming_runtime_design.md)
- current maintained working plan:
  - [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)

## Current Working Plan

- [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md)
  - current status board for the core runtime columnar path
  - tracks implemented items, explicit non-goals, and next phases
  - this is the main plan document to update for the active core-runtime line
- [agentic-event-data-platform-v1.md](./agentic-event-data-platform-v1.md)
  - top-level design for the local agentic event data platform direction
- [agentic-user-journeys-v1.md](./agentic-user-journeys-v1.md)
  - primary user journeys for external agents, skills, configurators, and analysts
- [agentic-event-model-v1.md](./agentic-event-model-v1.md)
  - domain model source of truth for observation, signal, focus-event, and monitor objects
- [agentic-search-grounding-v1.md](./agentic-search-grounding-v1.md)
  - search and grounding design for monitor creation, skill usage, and event reuse
- [agentic-monitor-execution-v1.md](./agentic-monitor-execution-v1.md)
  - execution semantics for batch monitors, stream monitors, and external event sources
- [agentic-rule-dsl-v1.md](./agentic-rule-dsl-v1.md)
  - rule DSL syntax, validation, and compilation targets for event monitors
- [analytical-storage-format-v1.md](./analytical-storage-format-v1.md)
  - v1 analytical storage format direction for internal saved tables
- [visual-data-app-v1.md](./visual-data-app-v1.md)
  - product-level positioning for the local visual data app
- [visual-data-app-shape-v1.md](./visual-data-app-shape-v1.md)
  - app information architecture and top-level page shape
- [visual-data-app-workflows-v1.md](./visual-data-app-workflows-v1.md)
  - import / data detail / analyze workflow design
- [electron-dmg-packaging-v1.md](./electron-dmg-packaging-v1.md)
  - Electron desktop packaging plan and `.dmg` delivery path

The agentic/event documents above are the current design source of truth for this product line.
They are still design documents in `plans/`, not yet stable `docs/` contracts.

## Historical Notes Still Useful As Background

- [stream-sql-v1.md](./stream-sql-v1.md)
  - design background for the current stream SQL subset and rejection rules
- [python-api-v1.md](./python-api-v1.md)
  - design background for the Python binding, Arrow ingress, and CLI-facing shape
- [ai-plugin-interface-v1.md](./ai-plugin-interface-v1.md)
  - hook-layer design note for the AI plugin integration path
- [build-system.md](./build-system.md)
  - earlier build-system rationale; the repository already uses Bazel today

These notes can still help explain why some implemented surfaces look the way they do, but they are not the primary status board. Their headers should state current role, implemented carry-over, and relation to the maintained plan.

## Older Roadmaps And Superseded Planning Notes

- [dataframe-first-v1.md](./dataframe-first-v1.md)
  - early roadmap from the DataFrame-first stage
- [streaming-first-roadmap.md](./streaming-first-roadmap.md)
  - early streaming-priority roadmap
- [review-dataframe-first-v1.md](./review-dataframe-first-v1.md)
  - review notes for the earlier DataFrame-first plan

These files should be treated as historical context unless parts of them are deliberately folded into the current maintained plan.

## Update Rule

- update the root README files when the repository-facing implementation scope changes
- update `docs/` when a stable contract or boundary changes
- update [core-runtime-columnar-plan.md](./core-runtime-columnar-plan.md) when the active core-runtime columnar work status changes
- do not silently turn older plan notes into the current status board
