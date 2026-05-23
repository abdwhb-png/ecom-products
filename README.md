# Fast Fashion Dashboard

Small HTML/CSS/JS dashboard for browsing fast-fashion product datasets through a lightweight Python API.

## Features

- dataset switcher
- search
- category filter
- sort options
- pagination
- image-only filter
- multi-image carousel when multiple product images exist
- local JSON API served by `server.py`
- external-consumer friendly read API for categories and products
- OpenAPI spec at `/openapi.json` (with backward-compatible alias `/api/openapi.json`)

## Project files

- `index.html` — frontend shell
- `styles.css` — UI styles
- `app.js` — dashboard logic
- `server.py` — local API + static file server
- `build_catalog.py` — builds `catalog.db` from source datasets
- `check_image_availability.py` — audits product image URLs and stores results in `image_status`
- `screenshot.js` — optional Playwright screenshot helper

## Datasets currently retained

Only these datasets are exposed by the local API:

- `shein` — Shein Bright Data sample
- `asos` — ASOS Hugging Face sample

The generated SQLite catalog is local-only and intentionally ignored by git.

## Run locally

```bash
cd fast-fashion-dashboard
python3 build_catalog.py
python3 server.py
```

By default the app serves on `127.0.0.1:8765` and automatically falls forward to the next free port if needed.

### Custom host / port

```bash
FAST_FASHION_HOST=0.0.0.0 FAST_FASHION_PORT=8765 python3 server.py
```

## Persistent local dev service

For a stable local process, use the bundled wrapper plus the systemd user unit:

- `scripts/run-dev-server.sh`
- `deploy/systemd/fast-fashion-dashboard.service`

Install it locally with:

```bash
mkdir -p ~/.config/systemd/user
cp deploy/systemd/fast-fashion-dashboard.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now fast-fashion-dashboard.service
systemctl --user status fast-fashion-dashboard.service --no-pager
```

If your environment does not expose a user bus, you can still run the wrapper directly:

```bash
./scripts/run-dev-server.sh
```

## API protection

In production, protect every `/api/*` route with a Bearer token:

```bash
FAST_FASHION_API_TOKEN="change-me-to-a-long-random-secret" python3 server.py
```

Then every API request must include:

```http
Authorization: Bearer change-me-to-a-long-random-secret
```

The dashboard stores the token in browser localStorage after you enter it once, so the UI can keep calling the protected API.
If `FAST_FASHION_API_TOKEN` is empty, the API stays open for local/dev usage.

Important access behavior:

- `/` (the HTML dashboard) is public unless you separately protect it at the reverse proxy level
- `/api/*` requires `Authorization: Bearer <token>` when `FAST_FASHION_API_TOKEN` is set, except the backward-compatible OpenAPI alias `/api/openapi.json`
- `/openapi.json` is intentionally public for client tooling and integration
- `/healthz` is intentionally public so Dokploy / reverse proxies can health-check the service without the API token
- on the first protected dashboard load, the browser asks for the token and then reuses it from localStorage

## API

### `GET /openapi.json`
Returns the public OpenAPI document.

### `GET /api/openapi.json`
Backward-compatible alias to the same OpenAPI document.

### `GET /api/datasets`
Returns dataset metadata.

### `GET /api/categories`
Returns stable category resources.
This endpoint is the dedicated source for category filters in the frontend.

Query params:
- `dataset`
- `page`
- `pageSize`

### `GET /api/categories/{slug}`
Returns a single category resource.

Query params:
- `dataset`

### `GET /api/products`
Returns products only.
This endpoint no longer embeds categories; use `/api/categories` separately.

Query params:
- `dataset`
- `search`
- `category`
- `sort`
- `imagesOnly` — only products that have at least one source image in the imported catalog
- `savedOnS3` — only products whose runtime S3 state is marked as saved in SQLite

Dashboard notes:
- the category stat on the homepage reflects the total number of available categories loaded from `/api/categories`, not only the first UI page of category options
- the S3 job detail modal paginates processed items 50 per page and uses a fixed top-right close icon
- `page`
- `pageSize`
- `format` (`legacy` or `resource`)

### `GET /api/products/{goods_id}`
Returns a single product resource.

Query params:
- `dataset`

## Compatibility guarantees

- category and product resources use `snake_case`
- money fields are strings with 2 decimals
- nested values remain real arrays/objects
- `goods_id` is stable and unique per dataset
- `source_url` is preferred for categories
- `category_url` is preferred for products

## Docker Compose / Dokploy

Local compose for validation:

```bash
docker compose up --build -d
```

Then test the dedicated health endpoint and the protected API:

```bash
curl http://127.0.0.1:8765/healthz
curl -H "Authorization: Bearer $FAST_FASHION_API_TOKEN" http://127.0.0.1:8765/api/datasets
```

Stop it when done:

```bash
docker compose down
```

For Dokploy, use `dokploy.compose.yml`.
Recommended Dokploy settings:

- **Mode:** Docker Compose
- **Compose file:** `dokploy.compose.yml`
- **Domain target port:** `8765`
- **Health check:** `/healthz`
- **Environment variables:** set them in Dokploy itself; `dokploy.compose.yml` is wired to read injected Dokploy env vars directly, so a committed `.env` file is not required on the server
- **Minimum required env:** `FAST_FASHION_API_TOKEN`
- **Persistence:** `../files/fast-fashion-dashboard/runtime` and `../files/fast-fashion-dashboard/data`

The container bootstraps the SQLite catalog automatically on first start if `/app/runtime/catalog.db` does not exist yet.
Datasets are therefore not baked into the image itself; on first deploy the container downloads/rebuilds the retained datasets and creates the SQLite catalog in the persistent runtime volume. On later redeploys, the existing catalog is reused.

Access notes for first deploy:

- the first boot can take longer because the catalog is built/downloaded before the service becomes healthy
- do not expect `/api/*` to work without the Bearer token when protection is enabled
- if you want the HTML dashboard itself to be private, protect the domain at Dokploy / Traefik / upstream proxy level too

## Deployment notes

For deployment, do not commit local datasets, secrets, or generated SQLite files unless you intentionally want them in the repo.
You can rebuild the SQLite catalog during deploy or replace the current ingestion pipeline with your own API/database layer later.

Recommended for prod:

- set `FAST_FASHION_API_TOKEN` to a long random secret
- keep the app behind HTTPS so the Bearer token is encrypted in transit
- use `/healthz` as the deployment health check
- keep `.env` out of git and inject secrets through your deploy platform when possible
- remember that the API token protects the API, not the dashboard HTML itself
