# sec-company-facts

Serving layer (Dolt SQL server + widgets API) for SEC company facts.

The container runs `dolt sql-server` plus the FastAPI/uvicorn widgets app. It
does **not** bake the database into the image — on first start it clones the
public DoltHub repo `deeleeramone/sec-company-facts` into a volume and then
stays current via Dolt read replication.

> There are **two serving variants** of the same widgets API:
> 1. **Dolt** (this image, default) — `dolt sql-server` + the app.
> 2. **DuckDB-over-parquet** — no Dolt, no MySQL: the repo is exported to parquet
>    and queried directly with embedded DuckDB. See
>    [DuckDB-over-parquet variant](#duckdb-over-parquet-variant) below. Both
>    variants return identical widget output.

## Run the published image

```bash
docker run -d \
  --name sec-company-facts \
  -p 8000:8000 \
  -v sec_dolt_data:/data \
  ghcr.io/deeleeramone/sec-company-facts:latest
```

Two things are required and are the usual gotchas:

- **`-p 8000:8000`** — publishes the HTTP port to the host. `EXPOSE` in the image
  only documents the port; it does not publish it. Without `-p` nothing is
  reachable from outside the container. Map a different host port with
  `-p 9000:8000`.
- **`-v sec_dolt_data:/data`** — a **named** volume so the multi-GB clone is
  downloaded once and reused on every run. Without a named volume each
  `docker run` gets a fresh anonymous volume and re-clones from DoltHub. The
  first start downloads the DB (watch progress with `docker logs -f sec-company-facts`);
  later starts reuse the volume and only pull the delta.

App is then at <http://localhost:8000>.

## Run with Docker Compose

```bash
docker compose pull   # or: docker compose build  (build locally)
docker compose up -d
```

Compose already publishes the port and declares the reusable `sec_dolt_data`
volume. Override defaults via env vars: `WIDGETS_PORT`, `WIDGETS_IMAGE`,
`DOLT_REMOTE`, `WIDGETS_MEMORY_LIMIT`, etc. To serve an existing host clone
instead of cloning from DoltHub, see the comment in [docker-compose.yml](docker-compose.yml).

## Clone size & memory (small cloud hosts)

The first start downloads the DB, which can spike RAM on memory-constrained
hosts. Two levers control this:

- **`DOLT_CLONE_DEPTH`** (default `1`) — a **shallow clone**: only the latest
  commit's data is downloaded, not full history. Far less to stream/buffer/index,
  so peak RAM and clone time stay low. History operations (`dolt diff` across old
  commits) won't work, which is irrelevant for serving. Set `DOLT_CLONE_DEPTH=`
  (empty) for a full-history clone. Dolt exposes no download-concurrency/chunk
  knob — depth is the way to shrink the transfer.
- **`GOMEMLIMIT`** (default `1536MiB`) / **`GOGC`** (default `30`) — Go's soft
  memory cap and GC aggressiveness, applied to the `dolt clone` process. Lower
  `GOMEMLIMIT` (e.g. `1024MiB`) to force harder GC and stay under a tight RAM
  ceiling — slower and more CPU, but it won't OOM. Keep it a few hundred MB below
  the container's memory limit to leave headroom for uvicorn/python.

```bash
docker run -d -p 8000:8000 -v sec_dolt_data:/data \
  -e DOLT_CLONE_DEPTH=1 -e GOMEMLIMIT=1024MiB \
  ghcr.io/deeleeramone/sec-company-facts:latest
```

## Staying up to date

- **On start:** the entrypoint runs `dolt pull` so the served data is current.
- **While running:** Dolt read replication (`dolt_read_replica_remote=origin`)
  auto-pulls from DoltHub at transaction start.
- Upstream data is refreshed nightly by [.github/workflows/nightly-dolthub.yml](.github/workflows/nightly-dolthub.yml).

## Publishing the image

Run the **Publish container image** workflow ([.github/workflows/publish-image.yml](.github/workflows/publish-image.yml))
manually (`workflow_dispatch`). It builds a small, code-only image — the data is
fetched at runtime, never baked in.

## DuckDB-over-parquet variant

A second image serves the **identical** widgets API with **no Dolt at serving
time**. The DoltHub repo is exported to parquet (via the DoltHub REST API — no
clone), published as a downloadable bundle, and the container queries the parquet
directly with embedded DuckDB. Backends are selected by `SEC_BACKEND` (`dolt`
default, `duckdb` for this variant); the app code is shared.

### Run it

```bash
docker compose -f docker-compose.duckdb.yml up -d
```

On first start the container downloads the published parquet bundle (the
`parquet-latest` GitHub Release, refreshed nightly) into a named volume and serves
it. App is at <http://localhost:8000>. Levers:

- **`PARQUET_MANIFEST_URL`** — the bundle's `manifest.json` URL. On each start the
  container compares the remote `data_version` to what's on the volume and only
  re-downloads when it changed. Set it to an **empty string** and mount your own
  parquet dir at `/data/parquet` to serve a local export instead.
- **`DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT`** — optional DuckDB tuning (default:
  auto-detect).

### Export the parquet bundle yourself

`python -m sec_app.export_parquet` writes typed, cik-sorted parquet plus a
`manifest.json`. The default source is the **DoltHub REST API — no clone, no
sql-server, no dolt binary**:

```bash
# Full-table CSV for regular tables + HEX(payload) over the JSON API for the one
# blob table. This is what CI uses.
python -m sec_app.export_parquet --source rest --out ./parquet

# Small CIK-sliced test export (big tables limited to a few CIKs).
python -m sec_app.export_parquet --source rest --out ./parquet-slice --ciks 320193,789019,1045810

# Optional: read from a running Dolt/MySQL sql-server instead (pymysql streaming).
python -m sec_app.export_parquet --source server --server 127.0.0.1:3306/sec_company_facts --out ./parquet
```

The two large tables (`facts_enc`, `standardized_statements_enc`) are sorted by
`cik` and sharded to stay under GitHub's 2 GiB/asset limit, so per-CIK lookups
prune on row-group statistics. (Note: DuckDB httpfs is deliberately **not** used
for the REST source — DoltHub ignores HTTP Range, so each table's CSV is fetched
with a single plain GET to `--tmp-dir`; DuckDB memory is capped so the cik sort
spills to disk instead of exhausting the host.)

### Refresh & publish

The dedicated [Build DuckDB parquet bundle workflow](.github/workflows/build-parquet-bundle.yml)
runs on its own runner after each successful nightly refresh (or on demand),
exports parquet **from the DoltHub REST API — no clone** — and uploads the bundle
to the `parquet-latest` Release. It is kept entirely separate from the
[nightly refresh](.github/workflows/nightly-dolthub.yml) so the IO/memory-heavy
conversion never competes with the SEC pipeline or `dolt gc`. Build/publish the
image with the **Publish DuckDB container image** workflow
([.github/workflows/publish-duckdb-image.yml](.github/workflows/publish-duckdb-image.yml)),
which tags `duckdb-latest` / `duckdb-<date>`.
