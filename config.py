"""
config.py — ApnaDukaan configuration

Load settings from environment variables with safe defaults.
For production, set these via your hosting platform's env config
or a .env file loaded by your process manager.
"""
import os
import secrets


class Config:
    # ── Security ──────────────────────────────────────────────────────────────
    # MUST be overridden in production via SECRET_KEY env var.
    # A random default is generated each startup if not set — this invalidates
    # all sessions on restart, so always set it explicitly in production.
    SECRET_KEY     = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
    ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")  # CHANGE in production

    # ── Database ──────────────────────────────────────────────────────────────
    DB_PATH = os.environ.get("DB_PATH", "apnadukaan.db")

    # ── Session ───────────────────────────────────────────────────────────────
    SESSION_COOKIE_HTTPONLY  = True   # JS cannot read session cookie
    SESSION_COOKIE_SAMESITE  = "Lax"  # CSRF mitigation
    SESSION_COOKIE_SECURE    = os.environ.get("HTTPS", "false").lower() == "true"
    PERMANENT_SESSION_LIFETIME = 86400 * 7  # 7 days

    # ── Rate limiting (simple in-memory, per-IP) ──────────────────────────────
    RATE_LIMIT_ENABLED       = True
    RATE_LIMIT_LOGIN         = 10   # max login attempts per window
    RATE_LIMIT_REGISTER      = 5    # max register attempts per window
    RATE_LIMIT_ORDER         = 30   # max orders per window
    RATE_LIMIT_WINDOW        = 3600  # window size in seconds (1 hour)

    # ── Input limits ──────────────────────────────────────────────────────────
    MAX_NAME_LEN             = 80
    MAX_ADDRESS_LEN          = 200
    MAX_PRODUCT_NAME_LEN     = 100
    MAX_STORE_NAME_LEN       = 100
    MAX_USERNAME_LEN         = 40
    MAX_PASSWORD_LEN         = 128

    # ── Business rules ────────────────────────────────────────────────────────
    MAX_CART_ITEMS           = 20
    MAX_PRODUCT_PRICE        = 100_000
    MAX_PRODUCT_QTY          = 100

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL  = os.environ.get("LOG_LEVEL", "INFO")
    LOG_FILE   = os.environ.get("LOG_FILE", "logs/apnadukaan.log")


class DevelopmentConfig(Config):
    DEBUG             = True
    SESSION_COOKIE_SECURE = False


class ProductionConfig(Config):
    DEBUG             = False
    SESSION_COOKIE_SECURE = os.environ.get("HTTPS", "false").lower() == "true"
    TESTING           = False


config = {
    "development": DevelopmentConfig,
    "production":  ProductionConfig,
    "default":     DevelopmentConfig,
}
