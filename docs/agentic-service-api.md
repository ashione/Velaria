# Velaria Agentic Service API

## Summary

This document describes the current local agentic service surface implemented by
`python/velaria_service/`.

It is the repository-facing reference for:

- external event source registration
- external event ingestion payload shape
- monitor creation and lifecycle endpoints
- focus-event and signal consumption endpoints
- current realtime processing semantics from the local sidecar

This is a Python ecosystem / local service document.
It is not part of the core kernel runtime contract in `docs/runtime-contract.md`.

## Scope

The current service is a local HTTP API served by `velaria-service`.

The service owns:

- local `external_event` source definitions
- event normalization into standard event rows
- monitor CRUD and intent-based creation
- monitor validation, enable/disable, and manual run
- focus-event listing and polling
- signal listing
- search and grounding endpoints for templates/events/datasets/fields

The service does not currently define:

- a network-distributed control plane
- a public cloud API
- push subscription delivery such as WebSocket/SSE/webhook callbacks

Current event delivery is pull-based:

- `GET /api/v1/focus-events`
- `POST /api/v1/focus-events/poll`

## Concepts

### External Event Source

An `external_event` source defines the schema contract for realtime JSON events
ingested from outside Velaria.

Minimum shape:

```json
{
  "source_id": "market_ticks",
  "name": "Market Ticks",
  "schema_binding": {
    "time_field": "ts",
    "type_field": "kind",
    "key_field": "symbol",
    "field_mappings": {
      "price": "price"
    }
  }
}
```

The source definition maps raw payload fields onto the standard event columns
used by monitor execution.

### Standard Event Columns

All external events are normalized into at least these columns:

- `event_time`
- `event_type`
- `source_key`
- `payload_json`
- `ingested_at`
- `source_id`

Additional mapped fields are exposed using the keys from `field_mappings`.

Example:

Raw event payload:

```json
{
  "ts": "2026-04-18T14:30:00Z",
  "kind": "tick",
  "symbol": "BTC",
  "price": 68000
}
```

Standardized row:

```json
{
  "event_time": "2026-04-18T14:30:00Z",
  "event_type": "tick",
  "source_key": "BTC",
  "payload_json": {
    "ts": "2026-04-18T14:30:00Z",
    "kind": "tick",
    "symbol": "BTC",
    "price": 68000
  },
  "ingested_at": "2026-04-18T14:30:01Z",
  "source_id": "market_ticks",
  "price": 68000
}
```

### Monitor

A monitor binds a source, a template/DSL rule, and an execution mode:

- `batch`
- `stream`

For `external_event` sources, the canonical stream-facing columns are the
standard event columns listed above.

Monitor rules should use canonical column names such as:

- `source_key`
- `event_type`
- mapped fields like `price`

They should not use the raw source binding field names such as `symbol` or
`kind` inside the compiled rule.

### FocusEvent

A `FocusEvent` is the main consumable event object emitted after a monitor rule
produces result rows and the promotion rule upgrades them into a durable event.

Each focus event links back to:

- `run_id`
- `artifact_ids`
- key fields and summary

## Realtime Processing Shape

Current realtime path:

```text
external event ingest
  -> normalized observation
  -> realtime Arrow push into local stream source
  -> long-running StreamingQuery
  -> result rows
  -> Signal
  -> FocusEvent
```

Current delivery characteristics:

- realtime ingest: supported
- realtime processing: supported
- focus-event delivery: pull-based
- subscription push delivery: not yet supported

## Endpoints

### Health

- `GET /health`

Returns local service readiness metadata.

### External Event Sources

- `GET /api/v1/external-events/sources`
- `POST /api/v1/external-events/sources`
- `POST /api/v1/external-events/sources/<source_id>/ingest`

Create source example:

```json
{
  "source_id": "market_ticks",
  "name": "Market Ticks",
  "schema_binding": {
    "time_field": "ts",
    "type_field": "kind",
    "key_field": "symbol",
    "field_mappings": {
      "price": "price"
    }
  }
}
```

Ingest example:

```json
{
  "ts": "2026-04-18T14:30:00Z",
  "kind": "tick",
  "symbol": "BTC",
  "price": 68000
}
```

### Search / Grounding

- `POST /api/v1/search/templates`
- `POST /api/v1/search/events`
- `POST /api/v1/search/datasets`
- `POST /api/v1/search/fields`
- `POST /api/v1/grounding`

These endpoints serve monitor creation and reuse workflows.

### Monitors

- `GET /api/v1/monitors`
- `GET /api/v1/monitors/<monitor_id>`
- `POST /api/v1/monitors`
- `POST /api/v1/monitors/from-intent`
- `PATCH /api/v1/monitors/<monitor_id>`
- `DELETE /api/v1/monitors/<monitor_id>`
- `POST /api/v1/monitors/<monitor_id>/validate`
- `POST /api/v1/monitors/<monitor_id>/enable`
- `POST /api/v1/monitors/<monitor_id>/disable`
- `POST /api/v1/monitors/<monitor_id>/run`

`from-intent` example:

```json
{
  "name": "BTC burst count",
  "intent_text": "count events in a window",
  "source": {
    "kind": "external_event",
    "source_id": "market_ticks"
  },
  "execution_mode": "stream",
  "template_params": {
    "group_by": ["source_key", "event_type"],
    "count_threshold": 2
  }
}
```

Validation currently rejects invalid `group_by` fields for `external_event`
monitors and returns the canonical available column set.

### Focus Events

- `GET /api/v1/focus-events`
- `GET /api/v1/focus-events/<event_id>`
- `POST /api/v1/focus-events/poll`
- `POST /api/v1/focus-events/<event_id>/consume`
- `POST /api/v1/focus-events/<event_id>/archive`

Polling example:

```json
{
  "consumer_id": "agent",
  "limit": 20
}
```

### Signals

- `GET /api/v1/signals`
- `GET /api/v1/signals/<signal_id>`

## Current UX Notes

The current desktop prototype exposes source creation and monitor creation in the
Monitors page, but the underlying semantics are:

1. create an external event source schema
2. create and validate a monitor against canonical event columns
3. enable the monitor
4. ingest events
5. poll or inspect focus events

The current UI is a local prototype and does not yet expose a first-class
"send test event" control or push subscription delivery.

## Relationship To Plans

For design intent, future scope, and non-contract modeling detail, see:

- `plans/agentic-event-data-platform-v1.md`
- `plans/agentic-user-journeys-v1.md`
- `plans/agentic-event-model-v1.md`
- `plans/agentic-search-grounding-v1.md`
- `plans/agentic-monitor-execution-v1.md`
- `plans/agentic-rule-dsl-v1.md`
