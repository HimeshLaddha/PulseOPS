# API Contracts

This document establishes the frozen API contracts for PulseOps Day 1.

---

## 1. Ingest Telemetry Spans

### `POST /v1/telemetry/spans`

Ingests an atomic batch of telemetry spans for a given service. If any span in the batch fails validation, the entire request is rejected with `HTTP 422` and zero spans are inserted.

#### Headers
- `Content-Type: application/json`

#### Request Body (`SpanBatchIngestRequest`)
```json
{
  "service_name": "checkout-service",
  "spans": [
    {
      "span_id": "4a7f9b2c1d8e3f01",
      "trace_id": "0123456789abcdef0123456789abcdef",
      "parent_span_id": null,
      "operation_name": "process_checkout",
      "start_time_unix_nano": 1718000000000000000,
      "end_time_unix_nano": 1718000000250000000,
      "status_code": "OK",
      "attributes": {
        "http.method": "POST",
        "http.status_code": 200,
        "customer.tier": "platinum"
      }
    },
    {
      "span_id": "8b3e2a1c9f4d7e02",
      "trace_id": "0123456789abcdef0123456789abcdef",
      "parent_span_id": "4a7f9b2c1d8e3f01",
      "operation_name": "charge_credit_card",
      "start_time_unix_nano": 1718000000050000000,
      "end_time_unix_nano": 1718000000200000000,
      "status_code": "OK",
      "attributes": {
        "payment.provider": "stripe",
        "amount": 99.95
      }
    }
  ]
}
```

#### Field Specifications
| Field | Type | Required | Constraints |
|---|---|---|---|
| `service_name` | String | Yes | Non-empty, max 128 characters |
| `spans` | Array | Yes | Minimum 1 element (`min_length=1`) |
| `spans[].span_id` | String | Yes | Exact 16-character lowercase hex (`^[0-9a-f]{16}$`) |
| `spans[].trace_id` | String | Yes | Exact 32-character lowercase hex (`^[0-9a-f]{32}$`) |
| `spans[].parent_span_id` | String / Null | No | Optional 16-character lowercase hex (`^[0-9a-f]{16}$`) or `null` |
| `spans[].operation_name` | String | Yes | Non-empty, max 256 characters |
| `spans[].start_time_unix_nano` | Integer | Yes | Signed 64-bit int (`-9223372036854775808` to `9223372036854775807`) |
| `spans[].end_time_unix_nano` | Integer | Yes | Signed 64-bit int; must be `>= start_time_unix_nano` |
| `spans[].status_code` | Enum | Yes | Literal `OK`, `ERROR`, or `UNSET` |
| `spans[].attributes` | Object | No | Key-value dictionary, defaults to `{}` |

#### Success Response (`HTTP 201 Created`)
```json
{
  "status": "accepted",
  "inserted_count": 2
}
```

#### Error Response (`HTTP 422 Unprocessable Entity`)
Returned on schema errors, regex violations, empty span lists, or `end_time < start_time`.
```json
{
  "detail": [
    {
      "loc": ["body", "spans", 0, "span_id"],
      "msg": "String should match pattern '^[0-9a-f]{16}$'",
      "type": "string_pattern_mismatch"
    }
  ]
}
```

---

## 2. Retrieve Trace by ID

### `GET /v1/traces/{trace_id}`

Retrieves all spans associated with a given `trace_id`, ordered chronologically (`start_time_unix_nano ASC`).

#### Parameters
- `trace_id` (path): Exact 32-character lowercase hex (`^[0-9a-f]{32}$`).

#### Success Response (`HTTP 200 OK`)
```json
{
  "trace_id": "0123456789abcdef0123456789abcdef",
  "service_name": "checkout-service",
  "span_count": 2,
  "spans": [
    {
      "span_id": "4a7f9b2c1d8e3f01",
      "trace_id": "0123456789abcdef0123456789abcdef",
      "parent_span_id": null,
      "service_name": "checkout-service",
      "operation_name": "process_checkout",
      "start_time_unix_nano": 1718000000000000000,
      "end_time_unix_nano": 1718000000250000000,
      "duration_ms": 250.0,
      "status_code": "OK",
      "attributes": {
        "http.method": "POST",
        "http.status_code": 200,
        "customer.tier": "platinum"
      }
    },
    {
      "span_id": "8b3e2a1c9f4d7e02",
      "trace_id": "0123456789abcdef0123456789abcdef",
      "parent_span_id": "4a7f9b2c1d8e3f01",
      "service_name": "checkout-service",
      "operation_name": "charge_credit_card",
      "start_time_unix_nano": 1718000000050000000,
      "end_time_unix_nano": 1718000000200000000,
      "duration_ms": 150.0,
      "status_code": "OK",
      "attributes": {
        "payment.provider": "stripe",
        "amount": 99.95
      }
    }
  ]
}
```

#### Note on Serialization
`duration_ms` is strictly serialized as an IEEE 754 floating-point number (e.g., `250.0`, not `"250.0"` or Decimal string).

#### Error Responses
- `HTTP 404 Not Found`:
```json
{
  "detail": "Trace 0123456789abcdef0123456789abcdef not found"
}
```
- `HTTP 422 Unprocessable Entity`: (Invalid `trace_id` regex pattern)
```json
{
  "detail": [
    {
      "loc": ["path", "trace_id"],
      "msg": "String should match pattern '^[0-9a-f]{32}$'",
      "type": "string_pattern_mismatch"
    }
  ]
}
```
