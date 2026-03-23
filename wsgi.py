"""
wsgi.py — WSGI entry point for production servers.

Usage:
    gunicorn -w 4 -b 0.0.0.0:8000 --access-logfile logs/access.log wsgi:app

Recommended gunicorn settings for small VPS (2-4 cores):
    -w $(( 2 * $(nproc) + 1 ))   workers = 2×CPU + 1
    --timeout 60
    --keep-alive 5
    --max-requests 1000           recycle workers to prevent memory leaks
    --max-requests-jitter 100
"""

import os
from app import create_app

app = create_app(os.environ.get("FLASK_ENV", "production"))
