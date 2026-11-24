"""Escape-room booking triage with LLM request spans, built with Pixeltable.

`Requests` holds one message sent to the venue per row. A computed column sends it to
gpt-4o-mini (JSON mode) and stores the raw completion; further computed columns pull out
the intent, urgency, summary and token usage, and a timedelta expression sets the
reply-by time.

`Spans` holds the OpenTelemetry spans that Pixeltable emitted while the pipeline ran
(insert, row evaluation, the OpenAI UDF call, catalog transactions, ...), loaded from the
span dump configured in pixeltable.toml. Computed columns derive the duration and area;
`LlmSpans` is a view over the model-call spans only.

The `@pxt.query` functions are the metrics view: per-span-name latency (count, mean, max
and a Python-side p95 aggregate), token usage per room, and a freshness check that uses
now() inside an aggregate. `Ticks` stores one metrics snapshot per pipeline run.

Layout: UDFs live in udfs.py; this file holds the table models and @pxt.query
functions. No HTTP router.

    python make_data.py
    pxt schema update app.py escape
    python pipeline.py run          # three ticks: insert a batch, triage it, load the spans
    python pipeline.py report       # streams the metrics with cursor()
"""

from __future__ import annotations

import datetime as dt

import pixeltable as pxt
import pixeltable.functions as pxtf
from pixeltable.functions import openai

from udfs import p95, parse_triage, span_area, span_ms, triage_messages

TableModel = pxt.model_base()

MODEL = 'gpt-4o-mini'


class Rooms(TableModel, name='rooms'):
    """One escape room."""

    room_id = pxt.Column(type=pxt.String, primary_key=True)
    name: pxt.String
    max_party: pxt.Int
    minutes: pxt.Int


class Requests(TableModel, name='requests'):
    """One guest message; triage runs on insert."""

    request_id = pxt.Column(type=pxt.String, primary_key=True)
    tick: pxt.String
    room: pxt.String
    party_size: pxt.Int
    received_at: pxt.Timestamp
    message: pxt.String

    completion = openai.chat_completions(
        triage_messages(message, room, party_size),
        model=MODEL,
        model_kwargs={'max_tokens': 80, 'temperature': 0, 'response_format': {'type': 'json_object'}},
    )
    triage = parse_triage(completion.choices[0].message.content)
    intent = triage.intent.astype(pxt.String)
    urgency = triage.urgency.astype(pxt.String)
    summary = triage.summary.astype(pxt.String)
    prompt_tokens = completion.usage.prompt_tokens.astype(pxt.Int)
    completion_tokens = completion.usage.completion_tokens.astype(pxt.Int)
    # timedelta arithmetic on a Timestamp column: answer within 2 hours
    reply_by = received_at + dt.timedelta(hours=2)
    received_day = received_at.date()


class Spans(TableModel, name='spans'):
    """One finished OpenTelemetry span, loaded from the span dump."""

    span_id = pxt.Column(type=pxt.String, primary_key=True)
    tick: pxt.String
    trace_id: pxt.String
    parent_id: pxt.String | None
    name: pxt.String
    started_at: pxt.Timestamp
    ended_at: pxt.Timestamp
    status: pxt.String
    attrs: pxt.Json
    events: pxt.Json | None

    duration_ms = span_ms(started_at, ended_at)
    area = span_area(name)


class LlmSpans(TableModel, name='llm_spans', base=Spans.where(Spans.name == 'pixeltable.udf.chat_completions')):
    """Only the spans of model calls (the OpenAI chat_completions UDF)."""

    udf_path = Spans.attrs['pxt.udf_path'].astype(pxt.String | None)
    column = Spans.attrs['pxt.column'].astype(pxt.String | None)
    # 'rate-limits:openai:gpt-4o-mini' -> 'gpt-4o-mini'
    model_name = pxtf.string.partition(Spans.attrs['pxt.resource_pool'].astype(pxt.String), ':openai:')[2]
    retries = Spans.attrs['pxt.retries'].astype(pxt.Int | None)
    # token usage arrives as a `pixeltable.udf.tokens` event on the UDF call span
    input_tokens = Spans.events[0].attributes['pxt.input_tokens'].astype(pxt.Int | None)
    output_tokens = Spans.events[0].attributes['pxt.output_tokens'].astype(pxt.Int | None)


@pxt.query
def llm_latency():
    """Model-call latency per model: calls, mean, max and p95 (ms), and retries."""
    s = LlmSpans
    return (
        s.group_by(s.model_name)
        .select(
            s.model_name,
            calls=pxtf.count(s.span_id),
            mean_ms=pxtf.mean(s.duration_ms),
            max_ms=pxtf.max(s.duration_ms),
            p95_ms=p95(s.duration_ms),
            retries=pxtf.sum(s.retries),
            input_tokens=pxtf.sum(s.input_tokens),
            output_tokens=pxtf.sum(s.output_tokens),
        )
        .order_by(s.model_name)
    )


@pxt.query
def span_metrics(tick: str):
    """Latency per span name for one tick: count, mean, max and p95 (a Python-side aggregate)."""
    s = Spans
    return (
        s.where(s.tick == tick)
        .group_by(s.name)
        .select(
            s.name,
            n=pxtf.count(s.span_id),
            mean_ms=pxtf.mean(s.duration_ms),
            max_ms=pxtf.max(s.duration_ms),
            p95_ms=p95(s.duration_ms),
        )
        .order_by(s.name)
    )


@pxt.query
def area_metrics():
    """All ticks: span count and p95 latency per area (catalog, store, plan, ...)."""
    s = Spans
    return s.group_by(s.area).select(s.area, n=pxtf.count(s.span_id), p95_ms=p95(s.duration_ms)).order_by(s.area)


@pxt.query
def token_usage():
    """Model calls and tokens per room."""
    r = Requests
    return (
        r.group_by(r.room)
        .select(
            r.room,
            calls=pxtf.count(r.request_id),
            prompt_tokens=pxtf.sum(r.prompt_tokens),
            completion_tokens=pxtf.sum(r.completion_tokens),
        )
        .order_by(r.room)
    )


@pxt.query
def freshness():
    """When the newest span ended, and the query time: now() inside an aggregate query."""
    s = Spans
    return s.select(newest=pxtf.max(s.ended_at), spans=pxtf.count(s.span_id), checked_at=pxtf.timestamp.now())


@pxt.query
def reply_queue(cutoff: dt.datetime):
    """Requests whose reply-by time is before the cutoff, most urgent first."""
    r = Requests
    return (
        r.where(r.reply_by < cutoff)
        .order_by(r.reply_by)
        .select(r.request_id, r.room, r.intent, r.urgency, r.reply_by, r.summary)
    )


class Ticks(TableModel, name='ticks'):
    """One pipeline run; the per-span metrics are computed and stored on insert."""

    tick = pxt.Column(type=pxt.String, primary_key=True)
    finished_at: pxt.Timestamp
    metrics = span_metrics(tick)
