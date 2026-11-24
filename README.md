<!-- pixeltable-example-app: 20261124-escape-room-traces -->
# Capture LLM request spans with OpenTelemetry in a metrics view in Python

Turn on Pixeltable's OpenTelemetry instrumentation (pip install 'pixeltable[otel]', then opentelemetry.instrumentation.pixeltable.init()) and every insert, row evaluation and model call becomes a span, written to a JSON-lines span dump and, if you set an OTLP endpoint, to Langfuse, Phoenix or any collector. This pipeline loads those spans back into a Pixeltable table next to the LLM outputs they describe, so latency per span name, p95, retries and token usage per model are ordinary group_by queries, and each scheduled run stores its own metrics snapshot. No separate tracing backend is required to answer how slow and how expensive the model calls were.

**Replaces:** Langfuse or Phoenix self-hosting + a separate results store

![escape-room bookings pipeline built with Pixeltable](.github/social-preview.png)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/pierrebrunelle/pixeltable-escape-room-traces?quickstart=1)
[![Built with Pixeltable](https://img.shields.io/badge/built%20with-Pixeltable-5b4bff)](https://pixeltable.com)
[![PyPI - pixeltable](https://img.shields.io/pypi/v/pixeltable?label=pixeltable)](https://pypi.org/project/pixeltable/)
[![GitHub stars](https://img.shields.io/github/stars/pixeltable/pixeltable?style=social)](https://github.com/pixeltable/pixeltable)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

An escape-room venue triages guest messages with gpt-4o-mini: a computed column on Requests sends each message in JSON mode and stores the completion, and further columns pull out intent, urgency, a short summary, token counts and a reply-by time (Timestamp + timedelta). pipeline.py turns on Pixeltable's OpenTelemetry instrumentation, writes every finished span to a JSON-lines dump, and after each scheduled tick loads the new spans into the Spans table, with computed duration and area columns and an LlmSpans view of the model calls (resource pool, retries and the token-usage event). The metrics are @pxt.query functions: per-span-name count, mean, max and a Python-side p95 aggregate, model latency and tokens, tokens per room, and a freshness check that uses now() inside an aggregate. Each tick stores its own metrics snapshot in Ticks; the report streams them with cursor() into Markdown. Needs an OpenAI API key (18 short gpt-4o-mini calls per full run).

[Pixeltable](https://pixeltable.com) is open-source, Python-native **multimodal AI data infrastructure**: tables, incremental computed columns, UDFs, indexes and serving in one library, running locally or on Pixeltable Cloud.

> ⭐ **Like this example?** Star [pixeltable/pixeltable](https://github.com/pixeltable/pixeltable) on GitHub. It helps other developers find it.

## What this example shows

- **Incremental computed columns** powered by plain Python UDFs (`@pxt.udf`)
- **Importable UDF module**: UDFs live in `udfs.py`; tables and queries live in `app.py` (Pixeltable resolves UDFs by module path)
- **`pixeltable.toml`** declares a local database and a **Pixeltable Cloud** database, so the same code deploys with `pxt db update`



## What's inside

| File | What it is |
|------|------------|
| `.devcontainer/devcontainer.json` | GitHub Codespaces / Dev Container config: Python 3.12, installs `requirements.txt` |
| `.github/social-preview.png` | Social preview image (1280x640) |
| `CITATION.cff` | Citation metadata (authors, license, release date, keywords) |
| `app.py` | Tables declared as Python classes and `@pxt.query` functions |
| `data/requests-t1.jsonl` | Sample data |
| `data/requests-t2.jsonl` | Sample data |
| `data/requests-t3.jsonl` | Sample data |
| `data/rooms.jsonl` | Sample data |
| `make_data.py` | Write the sample rooms and guest messages (fictional, offline) to data/ |
| `pipeline.py` | Scheduled triage pipeline that loads Pixeltable's OpenTelemetry spans into a table |
| `pixeltable.toml` | Project config: the local database plus a Pixeltable Cloud database (sizing, deploy excludes) |
| `udfs.py` | Pixeltable UDFs (`@pxt.udf`) in their own importable module, imported by `app.py` |
| `requirements.txt` / `pyproject.toml` | Dependencies (`pixeltable>=0.7.26`) |

**Tables**

| Table | Stored columns | Computed columns |
|-------|---------|------------------|
| `rooms` | `room_id`, `name`, `max_party`, `minutes` | - |
| `requests` | `request_id`, `tick`, `room`, `party_size`, `received_at`, `message` | `completion`, `triage`, `intent`, `urgency`, `summary`, `prompt_tokens`, `completion_tokens`, `received_day` |
| `spans` | `span_id`, `tick`, `trace_id`, `parent_id`, `name`, `started_at`, `ended_at`, `status`, `attrs`, `events` | `duration_ms`, `area` |
| `llm_spans` | - | `udf_path`, `column`, `retries`, `input_tokens`, `output_tokens` |
| `ticks` | `tick`, `finished_at` | `metrics` |

## Run in your browser (GitHub Codespaces)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/pierrebrunelle/pixeltable-escape-room-traces?quickstart=1)

1. Click **Open in GitHub Codespaces** above (or [this link](https://codespaces.new/pierrebrunelle/pixeltable-escape-room-traces?quickstart=1)). The dev container installs Python 3.12 and `pixeltable>=0.7.26` from `requirements.txt`.
2. In the codespace terminal, create the tables, seed them and run the app:

   ```bash
   pxt schema update app.py escape
   python make_data.py            # 3 rooms + 18 guest messages in three batches (fictional, offline)
   python pipeline.py run         # ticks t1..t3: insert, triage with gpt-4o-mini, load the spans (needs OPENAI_API_KEY)
   python pipeline.py report      # metrics per tick, model latency/tokens, p95 per area, reply queue -> output/metrics.md
   python pipeline.py slowest 10  # the slowest spans, streamed with cursor()
   python pipeline.py keys        # how many API keys the Pixeltable Cloud org has (pxt key list), names only
   ```


## Quickstart

Requires Python 3.11+ and `pixeltable>=0.7.26`.

```bash
git clone https://github.com/pierrebrunelle/pixeltable-escape-room-traces.git
cd pixeltable-escape-room-traces
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Create the tables in a local catalog directory named `escape`
pxt schema update app.py escape

python make_data.py            # 3 rooms + 18 guest messages in three batches (fictional, offline)
python pipeline.py run         # ticks t1..t3: insert, triage with gpt-4o-mini, load the spans (needs OPENAI_API_KEY)
python pipeline.py report      # metrics per tick, model latency/tokens, p95 per area, reply queue -> output/metrics.md
python pipeline.py slowest 10  # the slowest spans, streamed with cursor()
python pipeline.py keys        # how many API keys the Pixeltable Cloud org has (pxt key list), names only
```

## Deploy to Pixeltable Cloud

The same `app.py` runs on [Pixeltable Cloud](https://pixeltable.com). Sign in (or get a free trial database with `pxt new`), point the second database entry in `pixeltable.toml` at your own database, then deploy:

```bash
pxt login                       # or: export PIXELTABLE_API_KEY=<your-api-key>
# edit pixeltable.toml: name = 'pxt://<your-org>:<your-db>'
pxt db update pxt://<your-org>:<your-db>                 # build the image and upload the project
pxt schema update app.py pxt://<your-org>:<your-db>       # create the tables at the database root (no per-app folder)
```

Keep keys in environment variables or `pxt secret set`, never in code.

## Code walkthrough

**1. Business logic is plain Python, in `udfs.py`.** A `@pxt.udf` function can be used as a column expression. Pixeltable records UDFs by module path (`udfs.triage_messages`), so they live in their own importable module rather than inline in the app: the daemon, serving workers and Pixeltable Cloud import it again by that path.

```python
# udfs.py
@pxt.udf
def triage_messages(message: str, room: str, party_size: int) -> list[dict]:
    """Chat messages for the triage call: a fixed system prompt plus the guest's message."""
    return [
        {'role': 'system', 'content': TRIAGE_SYSTEM},
        {'role': 'user', 'content': f'Room: {room}. Party of {party_size}.\n\n{message}'},
    ]
```

**2. Tables are Python classes (`app.py`).** Annotated attributes are stored columns; attributes assigned an expression are **computed columns** (`completion`, `triage`, `intent`, `urgency`, `summary`, `prompt_tokens`, `completion_tokens`, `received_day`), evaluated incrementally on every insert or update and recomputed when their inputs change.

```python
# app.py
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
```

**3. Queries are functions (`app.py`).** `@pxt.query` wraps a Pixeltable query so it can be called from Python or exposed as a route:

```python
# app.py
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
```

## FAQ

**Do I still need Langfuse or Phoenix?**

Not to measure the pipeline. The spans land in a Pixeltable table and the metrics are @pxt.query functions. If you want a trace UI, set exporter_otlp_endpoint in the [otel] section of pixeltable.toml and the same spans also go to Langfuse, Phoenix, Jaeger or any OTLP collector.

**Which spans does Pixeltable emit?**

With span_level = 'debug': the API call (pixeltable.insert, pixeltable.get_table, ...), one pixeltable.row span per evaluated row, one pixeltable.udf.<name> span per UDF call (for example pixeltable.udf.chat_completions with the resource pool and retry count), plus catalog, planner and store spans. Model token usage is attached to the UDF span as a pixeltable.udf.tokens event.

**How is p95 latency computed?**

p95 in udfs.py is a @pxt.uda aggregate. It runs in Python next to the SQL aggregates count, mean and max in the same group_by query, one group per span name.

**How does it run on a schedule?**

pipeline.py tick <name> is one run: insert the batch (the triage column calls gpt-4o-mini on insert), load the new spans from the dump, and insert a Ticks row whose metrics column is computed by a @pxt.query on insert. Call it from cron, a CI schedule or any job runner.

**Does it work against Pixeltable Cloud?**

Yes: set ESCAPE_NS=pxt://<org>:<db>/escape. The tables, the triage column and the metrics queries run on the hosted database. The model calls then run on the hosted side, so the local span dump only holds the client-side spans.

## Learn more

- 🌐 Website: https://pixeltable.com
- 📚 Docs: https://docs.pixeltable.com
- 💻 Source: https://github.com/pixeltable/pixeltable (⭐ star it if Pixeltable is useful to you)
- 📦 PyPI: https://pypi.org/project/pixeltable/
- 🧩 More example apps: https://pierrebrunelle.github.io/awesome-pixeltable-apps/

**[More Pixeltable example apps →](https://pierrebrunelle.github.io/awesome-pixeltable-apps/)**

---

<sub>Built as part of a daily series of Pixeltable example apps · Pixeltable 0.7.26 · Python, incremental computed columns · Licensed under Apache-2.0.</sub>
