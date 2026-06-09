# sec-company-facts

Serving layer (Dolt SQL server + widgets API) for SEC company facts.

The container runs `dolt sql-server` plus the FastAPI/uvicorn widgets app. It
does **not** bake the database into the image — on first start it clones the
public DoltHub repo `deeleeramone/sec-company-facts` into a volume and then
stays current via Dolt read replication.

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
