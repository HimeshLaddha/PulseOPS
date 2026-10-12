from typing import Any, Literal, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

# PostgreSQL 64-bit signed integer boundaries
MIN_INT64 = -9_223_372_036_854_775_808
MAX_INT64 = 9_223_372_036_854_775_807

SPAN_ID_REGEX = r"^[0-9a-f]{16}$"
TRACE_ID_REGEX = r"^[0-9a-f]{32}$"


class SpanModel(BaseModel):
    span_id: str = Field(
        ...,
        pattern=SPAN_ID_REGEX,
        description="16-character lowercase hexadecimal span ID",
    )
    trace_id: str = Field(
        ...,
        pattern=TRACE_ID_REGEX,
        description="32-character lowercase hexadecimal trace ID",
    )
    parent_span_id: Optional[str] = Field(
        default=None,
        pattern=SPAN_ID_REGEX,
        description="Optional 16-character lowercase hexadecimal parent span ID",
    )
    operation_name: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Non-empty operation name up to 256 characters",
    )
    start_time_unix_nano: int = Field(
        ...,
        ge=MIN_INT64,
        le=MAX_INT64,
        description="Start time in nanoseconds since Unix epoch (signed 64-bit)",
    )
    end_time_unix_nano: int = Field(
        ...,
        ge=MIN_INT64,
        le=MAX_INT64,
        description="End time in nanoseconds since Unix epoch (signed 64-bit)",
    )
    status_code: Literal["OK", "ERROR", "UNSET"] = Field(
        default="OK",
        description="Span execution status",
    )
    attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary key-value metadata dictionary",
    )

    @field_validator("operation_name")
    @classmethod
    def validate_non_empty_operation_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("operation_name cannot be blank or whitespace-only")
        return v

    @model_validator(mode="after")
    def validate_timestamp_ordering(self) -> "SpanModel":
        if self.end_time_unix_nano < self.start_time_unix_nano:
            raise ValueError(
                f"end_time_unix_nano ({self.end_time_unix_nano}) must be greater than "
                f"or equal to start_time_unix_nano ({self.start_time_unix_nano})"
            )
        return self


class SpanBatchIngestRequest(BaseModel):
    service_name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Non-empty service name up to 128 characters",
    )
    spans: list[SpanModel] = Field(
        ...,
        min_length=1,
        description="List of spans to ingest; minimum 1 span required",
    )

    @field_validator("service_name")
    @classmethod
    def validate_non_empty_service(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("service_name cannot be blank or whitespace-only")
        return v


class SpanResponse(BaseModel):
    span_id: str
    trace_id: str
    parent_span_id: Optional[str] = None
    service_name: str
    operation_name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    duration_ms: float
    status_code: str
    attributes: dict[str, Any] = Field(default_factory=dict)


class TraceResponse(BaseModel):
    trace_id: str
    service_name: str
    span_count: int
    spans: list[SpanResponse]


class IngestResponse(BaseModel):
    status: Literal["accepted"] = "accepted"
    inserted_count: int
