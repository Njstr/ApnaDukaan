# ApnaDukaan v3 — Offline Retail Booking App

## What's New in v3
- **Product images** — owners upload JPEG/PNG/WebP per product (stored as base64, no filesystem needed)
- **Rich product pages** — image, price, description, 2-column specs table (brand, model, origin, dimensions, weight, material + 3 custom rows), reviews, AI summary, frequently bought together, people also browsed
- **Ratings & reviews** — customers rate stores (1–5 ★) and products after completing an order; store owners rate customers after delivery; both can see each other's ratings
- **Review management** — customers can edit/delete their own reviews; others' reviews are read-only
- **AI review summaries** — set `ANTHROPIC_API_KEY` to enable auto-generated 2–3 sentence summaries per product/store, cached 24 h
- **Date picker** — customers choose a pickup date (up to 7 days ahead) + time slot at checkout
- **Strict role separation** — one browser session cannot be both customer and store owner simultaneously

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env          # set SECRET_KEY at minimum
python app.py                 # dev server at http://localhost:5000
```

## Demo Account
- **Owner login**: username `demo` / password `demo123`
- **Customer**: type any name on the customer page

## Production
```bash
FLASK_ENV=production SECRET_KEY=$(python -c "import secrets;print(secrets.token_hex(32))") \
  gunicorn -w 4 -b 0.0.0.0:8000 --timeout 60 wsgi:app
```

## AI Review Summaries
Add `ANTHROPIC_API_KEY=sk-ant-...` to `.env`. Summaries are generated using claude-haiku-4-5 and cached for 24 hours. Gracefully skipped if key is absent.

## Key Routes

| Route | Description |
|-------|-------------|
| `/customer` | Customer name login |
| `/stores` | Browse all stores |
| `/store/<id>` | Store page with products + reviews |
| `/product/<id>` | Rich product detail page |
| `/checkout/<store_id>` | Date + slot picker + cart |
| `/my-orders` | Order history |
| `/my-points` | Trust points + ratings received |
| `/scan` | QR code verification |
| `/owner/register` | Store owner registration |
| `/owner/dashboard` | Today's order board |
| `/owner/products` | Product management |
| `/owner/product/add` | Add product with image + specs |
| `/owner/orders` | Orders by date |
| `/owner/analytics` | Revenue, peak slots, reviews |
| `/control-panel/` | Admin (not linked in UI) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | required | Flask session key |
| `FLASK_ENV` | `production` | `development` or `production` |
| `DB_PATH` | `apnadukaan.db` | SQLite path |
| `ANTHROPIC_API_KEY` | — | Enables AI review summaries |
| `ADMIN_PASSWORD` | `admin123` | Initial admin password |
| `LOG_LEVEL` | `INFO` | Logging level |
| `HTTPS` | `false` | Enables HSTS + Secure cookies |
| `PICKUP_DAYS_AHEAD` | `7` | How many days ahead customer can book |
