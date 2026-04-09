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

## Project files

- `index.html` — frontend shell
- `styles.css` — UI styles
- `app.js` — dashboard logic
- `server.py` — local API + static file server
- `build_catalog.py` — builds `catalog.db` from source datasets
- `check_image_availability.py` — audits product image URLs and stores results in `image_status`
- `screenshot.js` — optional Playwright screenshot helper

## Datasets currently expected

`build_catalog.py` currently looks for:

- local Kaggle CSV at `../data/shein-kaggle-111k.csv`
- remote Shein sample CSV from Bright Data GitHub sample
- remote Zara sample CSV

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

### `GET /api/datasets`
Returns dataset metadata.

### `GET /api/products`
Query params:

- `dataset`
- `search`
- `category`
- `sort`
- `imagesOnly`
- `page`
- `pageSize`

## Deployment notes

For deployment, do not commit local datasets, secrets, or generated SQLite files unless you intentionally want them in the repo.
You can rebuild the SQLite catalog during deploy or replace the current ingestion pipeline with your own API/database layer later.
