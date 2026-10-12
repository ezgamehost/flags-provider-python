# ezgh-flags-openfeature

The [OpenFeature](https://openfeature.dev) provider for **EZGH Feature Flags**, for Python servers.

It loads your environment's flags from the EZGH data plane (`https://flags.ezghcloud.com`) and
evaluates them **locally** with flagd's open-source core (`openfeature-flagd-core`), so an
evaluation never makes a network call. A background thread follows the data plane's stream and
swaps in each new snapshot as soon as a flag changes.

## Install

The provider installs from GitHub (it isn't on PyPI):

```bash
pip install "git+https://github.com/ezgamehost/flags-provider-python@v0.1.0"
# or
uv add "ezgh-flags-openfeature @ git+https://github.com/ezgamehost/flags-provider-python@v0.1.0"
```

Python 3.12 or later. It depends on `openfeature-sdk` 0.10 and `openfeature-flagd-core` 1.0.0, and
uses only the standard library for HTTP.

## Use

Create a **server** SDK key (`ezgh_fs_…`) for the environment in the console or with
`ezgh flags keys create --env <envKey> --kind server --name …`, and keep it secret.

```python
import os

from openfeature import api
from openfeature.evaluation_context import EvaluationContext

from ezgh_flags import EzghProvider

api.set_provider_and_wait(EzghProvider(os.environ["EZGH_FLAGS_SDK_KEY"]))
client = api.get_client()

ctx = EvaluationContext(targeting_key="user-1842", attributes={"plan": "pro", "country": "US"})
if client.get_boolean_value("new-checkout", False, ctx):
    ...
```

`targeting_key` (or a string `targetingKey` attribute, when `targeting_key` isn't set) is what individual targets, segments and percentage rollouts match on (unless a
rollout buckets by another attribute). Contexts are evaluated in your process and never sent
anywhere.

### Options

```python
EzghProvider(
    sdk_key,
    endpoint="https://flags.ezghcloud.com",  # the data plane
    bootstrap_path=None,  # a snapshot file from `ezgh flags export`
    offline=False,        # True: evaluate only bootstrap_path, never call the network
    timeout=10.0,         # seconds per HTTP request, and for initialize's retries
)
```

- **Bootstrap:** with `bootstrap_path`, the provider is ready at once from the file, then replaces
  it with the data plane's snapshot when it connects.
- **Offline:** `offline=True` (which needs `bootstrap_path`) never touches the network: for tests,
  CI and air-gapped runs.

```bash
ezgh flags export --env production > flags.json
```

## Behavior

| Situation | What the provider does |
| --- | --- |
| Start | Fetches `GET /v1/sync/snapshot`, then is `READY` and follows `POST /v1/sync/stream`. Transient errors (`429`, `503`, network) are retried until `timeout`; after that `initialize` fails with `GENERAL` and the provider keeps trying in the background, emitting `PROVIDER_READY` once loaded. |
| A new snapshot | Swaps it in and emits `PROVIDER_CONFIGURATION_CHANGED` (every new version, with `metadata.snapshotVersion`). `flags_changed` lists the flags whose compiled definition changed, including through a segment they use, and flags added or removed; it can be empty. A snapshot older than the current one is ignored. |
| The stream fails | Retries it with exponential backoff (1 s to 60 s, with jitter), honouring `Retry-After`. After 3 failures in a row it also polls the snapshot every 30 s with `If-None-Match`. |
| Disconnected for 30 s | Keeps evaluating the last snapshot and emits `PROVIDER_STALE`; `PROVIDER_READY` again once it reconnects. |
| A stream answers without a frame | Counts as a failed stream (as does a `200` that isn't `text/event-stream`). |
| `401`/`403`, the key is revoked, or its environment is deleted | `PROVIDER_ERROR` with `PROVIDER_FATAL`; it stops calling the data plane. |

Evaluation details carry `variant`, `reason` and `flag_metadata["flagVersion"]` (the flag's config
version in the environment). The reason is `DISABLED` for a flag that is off (serving its off
variation), `TARGETING_MATCH` when a target, rule or rollout served it, `DEFAULT` when rules exist
and none matched, and `STATIC` for a flag that is on without targeting. A flag asked for with the
wrong type answers your default with `TYPE_MISMATCH`.

Use `api.shutdown()` (or `provider.shutdown()`) to stop the background thread.

## Development

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy            # strict, src and tests
uv run pytest
```

`tests/vectors/` holds flags-stack's conformance vectors (`protocol/vectors/eval`), copied verbatim
by `scripts/sync-vectors.sh ~/code/flags-stack [<commit>]`, which also records the commit in
`.flags-stack-rev`. Every EZGH provider and the data plane must reproduce every case; change them in
flags-stack, never here. CI checks that the copies match flags-stack at `.flags-stack-rev`.

The provider tests run against a fake data plane (`tests/fake_data_plane.py`).

## License

Apache-2.0. See [LICENSE](LICENSE).
