"""Scheduled triage pipeline that loads Pixeltable's OpenTelemetry spans into a table.

Each tick inserts a batch of guest messages, lets the computed columns triage them with
gpt-4o-mini, loads the spans Pixeltable emitted into the `spans` table, and stores a
metrics snapshot.

    python pipeline.py tick t1      # one batch (data/requests-t1.jsonl)
    python pipeline.py run          # ticks t1..t3, as a scheduler (cron, CI) would call them
    python pipeline.py report       # stream the metrics with cursor(); writes output/metrics.md
    python pipeline.py slowest 10   # the slowest spans, streamed
    python pipeline.py keys         # how many API keys the org has (pxt key list), names only

Point ESCAPE_NS at a hosted database (pxt://<org>:<db>/escape) to run the same pipeline in
Pixeltable Cloud, and ESCAPE_SPAN_DUMP at a separate file so the two runs' spans stay apart.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

# Tracing must be configured before Pixeltable does any work: init() reads the [otel]
# section of pixeltable.toml (service name, span level) and writes finished spans to DUMP.
import opentelemetry.instrumentation.pixeltable as pxt_otel

APP = Path(__file__).parent
DATA = APP / 'data'
OUT = APP / 'output'
NS = os.environ.get('ESCAPE_NS', 'escape')
DUMP = Path(os.environ.get('ESCAPE_SPAN_DUMP', APP / 'logs' / 'spans.jsonl'))
OFFSET = DUMP.with_suffix('.offset')
DUMP.parent.mkdir(parents=True, exist_ok=True)
pxt_otel.init(span_dump=str(DUMP))

import pixeltable as pxt  # noqa: E402
from opentelemetry import trace  # noqa: E402


def _t(name: str):
    return pxt.get_table(f'{NS}/{name}')


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _ts(s: str) -> dt.datetime:
    # span dumps use RFC 3339 with a trailing Z
    return dt.datetime.fromisoformat(s.replace('Z', '+00:00'))


def _flush() -> None:
    provider = trace.get_tracer_provider()
    if hasattr(provider, 'force_flush'):
        provider.force_flush()


def load_spans(tick: str) -> int:
    """Append the spans written since the last load to the `spans` table."""
    _flush()
    if not DUMP.exists():
        return 0
    start = int(OFFSET.read_text()) if OFFSET.exists() else 0
    with DUMP.open('rb') as f:
        f.seek(start)
        chunk = f.read()
    end = start + chunk.rfind(b'\n') + 1
    rows = []
    for line in chunk[: end - start].decode().splitlines():
        d = json.loads(line)
        rows.append(
            {
                'span_id': d['context']['span_id'],
                'tick': tick,
                'trace_id': d['context']['trace_id'],
                'parent_id': d.get('parent_id'),
                'name': d['name'],
                'started_at': _ts(d['start_time']),
                'ended_at': _ts(d['end_time']),
                'status': d['status']['status_code'],
                'attrs': d.get('attributes') or {},
                'events': d.get('events') or None,
            }
        )
    if rows:
        _t('spans').insert(rows, if_exists='ignore')
    OFFSET.write_text(str(end))
    return len(rows)


def seed_rooms() -> None:
    _t('rooms').insert(_jsonl(DATA / 'rooms.jsonl'), if_exists='ignore')


def tick(name: str) -> None:
    seed_rooms()
    rows = _jsonl(DATA / f'requests-{name}.jsonl')
    for r in rows:
        r['received_at'] = dt.datetime.fromisoformat(r['received_at'])
    status = _t('requests').insert(rows, if_exists='ignore', on_error='ignore')
    print(f'{name}: inserted {status.num_rows} requests, {status.num_excs} errors')
    n = load_spans(name)
    _t('ticks').insert([{'tick': name, 'finished_at': dt.datetime.now(dt.timezone.utc)}], if_exists='ignore')
    print(f'{name}: loaded {n} spans')


def run() -> None:
    for name in ('t1', 't2', 't3'):
        tick(name)
    load_spans('t3')  # the spans of the last load itself


def _md_table(rows, cols) -> list[str]:
    out = ['| ' + ' | '.join(cols) + ' |', '|' + '---|' * len(cols)]
    for r in rows:
        out.append('| ' + ' | '.join(_fmt(r[c]) for c in cols) + ' |')
    return out


def _fmt(v) -> str:
    if isinstance(v, float):
        return f'{v:.2f}'
    if isinstance(v, dt.datetime):
        return v.isoformat(timespec='seconds')
    return str(v)


def report() -> None:
    import app

    app.TableModel.bind_all(NS)
    lines = ['# Escape-room triage: LLM request spans', '']

    ticks = _t('ticks')
    with ticks.select(ticks.tick, ticks.metrics).order_by(ticks.tick).cursor() as cur:
        for row in cur:
            llm = [m for m in row['metrics'] if m['name'].startswith('pixeltable.udf.') or m['name'] == 'pixeltable.insert']
            lines += [f'## Tick {row["tick"]}', '']
            lines += _md_table(llm, ['name', 'n', 'mean_ms', 'max_ms', 'p95_ms'])
            lines.append('')

    lines += ['## Model calls (all ticks)', '']
    lines += _md_table(pxt.eval_udf(app.llm_latency), ['model_name', 'calls', 'mean_ms', 'max_ms', 'p95_ms', 'retries', 'input_tokens', 'output_tokens'])
    lines += ['', '## Spans per area (all ticks)', '']
    lines += _md_table(pxt.eval_udf(app.area_metrics), ['area', 'n', 'p95_ms'])
    lines += ['', '## Tokens per room', '']
    lines += _md_table(pxt.eval_udf(app.token_usage), ['room', 'calls', 'prompt_tokens', 'completion_tokens'])
    fresh = pxt.eval_udf(app.freshness)
    lines += ['', f'Newest span ended {_fmt(fresh["newest"])}; {fresh["spans"]} spans; checked {_fmt(fresh["checked_at"])}.']
    cutoff = dt.datetime.fromisoformat('2026-11-24T11:00:00-08:00')
    lines += ['', f'## Replies due before {_fmt(cutoff)}', '']
    lines += _md_table(pxt.eval_udf(app.reply_queue, cutoff), ['request_id', 'room', 'intent', 'urgency', 'reply_by', 'summary'])
    OUT.mkdir(exist_ok=True)
    (OUT / 'metrics.md').write_text('\n'.join(lines) + '\n')
    print('\n'.join(lines))


def slowest(limit: int = 10) -> None:
    s = _t('spans')
    q = s.order_by(s.duration_ms, asc=False).limit(limit).select(s.tick, s.name, s.duration_ms)
    with q.cursor() as cur:
        for row in cur:
            print(f'{row["tick"]}  {row["duration_ms"]:9.2f} ms  {row["name"]}')


def keys() -> None:
    out = subprocess.run(['pxt', 'key', 'list', '--json'], capture_output=True, text=True, check=True).stdout
    data = json.loads(out)
    items = data if isinstance(data, list) else data.get('keys', data)
    print(f'{len(items)} keys in the organization; fields: {sorted(items[0]) if items else []}')


if __name__ == '__main__':
    cmd, *args = sys.argv[1:] or ['run']
    {'tick': tick, 'run': run, 'report': report, 'slowest': lambda n='10': slowest(int(n)), 'keys': keys}[cmd](*args)
