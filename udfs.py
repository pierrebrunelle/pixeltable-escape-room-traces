"""UDFs for the escape-room booking pipeline (pure Python, no table imports)."""

from __future__ import annotations

import datetime as dt
import json

import pixeltable as pxt

TRIAGE_SYSTEM = (
    'You triage messages sent to an escape-room venue. Reply with a JSON object with keys '
    '"intent" (one of: book, reschedule, cancel, question, complaint), "urgency" (low, normal, high) '
    'and "summary" (at most 12 words).'
)
INTENTS = ('book', 'reschedule', 'cancel', 'question', 'complaint')


@pxt.udf
def triage_messages(message: str, room: str, party_size: int) -> list[dict]:
    """Chat messages for the triage call: a fixed system prompt plus the guest's message."""
    return [
        {'role': 'system', 'content': TRIAGE_SYSTEM},
        {'role': 'user', 'content': f'Room: {room}. Party of {party_size}.\n\n{message}'},
    ]


@pxt.udf
def parse_triage(content: str | None) -> pxt.Json:
    """Parse the model's JSON reply; unknown intents fall back to 'question'."""
    try:
        d = json.loads(content or '{}')
    except json.JSONDecodeError:
        d = {}
    intent = str(d.get('intent', 'question')).lower()
    urgency = str(d.get('urgency', 'normal')).lower()
    return {
        'intent': intent if intent in INTENTS else 'question',
        'urgency': urgency if urgency in ('low', 'normal', 'high') else 'normal',
        'summary': str(d.get('summary', ''))[:120],
    }


@pxt.udf
def span_ms(started_at: dt.datetime, ended_at: dt.datetime) -> float:
    """Span duration in milliseconds."""
    return round((ended_at - started_at).total_seconds() * 1000.0, 3)


@pxt.udf
def span_area(name: str) -> str:
    """Coarse area of a Pixeltable span name: 'pixeltable.catalog.begin_xact' -> 'catalog'."""
    parts = name.split('.')
    if len(parts) < 2:
        return name
    return parts[1] if len(parts) > 2 else 'api'


@pxt.uda
class p95(pxt.Aggregator):
    """95th percentile (nearest rank) of a float column; runs in Python, not SQL."""

    def __init__(self) -> None:
        self.values: list[float] = []

    def update(self, value: float | None) -> None:
        if value is not None:
            self.values.append(value)

    def value(self) -> float | None:
        if not self.values:
            return None
        v = sorted(self.values)
        k = max(0, int(round(0.95 * len(v) + 0.5)) - 1)
        return v[min(k, len(v) - 1)]
