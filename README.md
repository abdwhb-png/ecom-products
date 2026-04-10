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
- OpenAPI spec at `/api/openapi.json`

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

## API

### `GET /api/openapi.json`
Returns the OpenAPI document.

### `GET /api/datasets`
Returns dataset metadata.

### `GET /api/categories`
Returns stable category resources.

Query params:
- `dataset`
- `page`
- `pageSize`

### `GET /api/categories/{slug}`
Returns a single category resource.

Query params:
- `dataset`

### `GET /api/products`
Returns products.

Query params:
- `dataset`
- `search`
- `category`
- `sort`
- `imagesOnly`
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

## Deployment notes

For deployment, do not commit local datasets, secrets, or generated SQLite files unless you intentionally want them in the repo.
You can rebuild the SQLite catalog during deploy or replace the current ingestion pipeline with your own API/database layer later.
