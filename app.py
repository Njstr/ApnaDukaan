"""
app.py — ApnaDukaan (production, v3)
New: product images, rich product pages, mutual ratings, AI review summaries, date picker.
Run dev:  python app.py
Run prod: gunicorn -w 4 -b 0.0.0.0:8000 wsgi:app
"""
import uuid
import math
import requests
from geopy.geocoders import Nominatim
import sqlite3, os, json, uuid, base64, io, re, html
import logging, logging.handlers, time, hashlib
from collections import defaultdict
from datetime import datetime, date, timedelta
from functools import wraps
from flask import (Flask, render_template, request, redirect,
                   url_for, session, jsonify, flash, g, abort)
from werkzeug.security import generate_password_hash, check_password_hash
try:
    import qrcode; QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False
from config import config as app_config

# ── APP FACTORY ───────────────────────────────────────────────────────────────
def create_app(env=None):
    app = Flask(__name__)
    env = env or os.environ.get("FLASK_ENV","production")
    app.config.from_object(app_config.get(env, app_config["default"]))
    app.config.setdefault("ANTHROPIC_API_KEY", os.environ.get("ANTHROPIC_API_KEY",""))
    app.config.setdefault("MAX_IMAGE_BYTES", 2*1024*1024)
    app.config.setdefault("PICKUP_DAYS_AHEAD", 7)
    _setup_logging(app); _register_db_hooks(app)
    _register_error_handlers(app); _register_security_headers(app)
    _register_routes(app)
    with app.app_context():
        init_db(app.config["DB_PATH"])
        # seed_demo_data(app.config["DB_PATH"])
    return app

# ── LOGGING ───────────────────────────────────────────────────────────────────
def _setup_logging(app):
    os.makedirs("logs", exist_ok=True)
    fmt   = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    level = getattr(logging, app.config.get("LOG_LEVEL","INFO"), logging.INFO)
    fh    = logging.handlers.RotatingFileHandler(
                app.config.get("LOG_FILE","logs/apnadukaan.log"),
                maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt); fh.setLevel(level)
    ch = logging.StreamHandler(); ch.setFormatter(fmt); ch.setLevel(level)
    app.logger.setLevel(level); app.logger.addHandler(fh)
    app.logger.addHandler(ch); app.logger.propagate = False

# ── DATABASE ──────────────────────────────────────────────────────────────────
def _register_db_hooks(app):
    @app.teardown_appcontext
    def close_db(exc=None):
        db = g.pop("db", None)
        if db: db.close()

def get_db():
    if "db" not in g:
        from flask import current_app
        g.db = sqlite3.connect(current_app.config["DB_PATH"], timeout=30, check_same_thread=False)
        g.db.row_factory = sqlite3.Row
        for pragma in ("PRAGMA journal_mode=WAL","PRAGMA busy_timeout=30000",
                       "PRAGMA synchronous=NORMAL","PRAGMA foreign_keys=ON"):
            g.db.execute(pragma)
    return g.db

def _direct_conn(db_path):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    for pragma in ("PRAGMA journal_mode=WAL","PRAGMA busy_timeout=30000","PRAGMA foreign_keys=ON"):
        conn.execute(pragma)
    return conn

# ── SCHEMA ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS admins(admin_id TEXT PRIMARY KEY,username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT(datetime('now')));
CREATE TABLE IF NOT EXISTS owners(owner_id TEXT PRIMARY KEY,
  username TEXT UNIQUE NOT NULL COLLATE NOCASE,password TEXT NOT NULL,
  full_name TEXT NOT NULL,phone TEXT,is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT(datetime('now')));
CREATE TABLE IF NOT EXISTS stores(store_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,
  name TEXT NOT NULL,owner_name TEXT NOT NULL,address TEXT NOT NULL DEFAULT'',
  category TEXT NOT NULL DEFAULT'General',qr_code TEXT NOT NULL,
  points INTEGER NOT NULL DEFAULT 0,is_open INTEGER NOT NULL DEFAULT 1,
  is_approved INTEGER NOT NULL DEFAULT 1,
  description TEXT NOT NULL DEFAULT'',
  image_b64 TEXT,image_mime TEXT NOT NULL DEFAULT'image/jpeg',
  created_at TEXT NOT NULL DEFAULT(datetime('now')),
  FOREIGN KEY(owner_id) REFERENCES owners(owner_id));
CREATE TABLE IF NOT EXISTS products(product_id TEXT PRIMARY KEY,store_id TEXT NOT NULL,
  name TEXT NOT NULL,price REAL NOT NULL CHECK(price>=0),unit TEXT NOT NULL DEFAULT'piece',
  available INTEGER NOT NULL DEFAULT 1,category TEXT NOT NULL DEFAULT'General',
  description TEXT NOT NULL DEFAULT'',image_b64 TEXT,image_mime TEXT NOT NULL DEFAULT'image/jpeg',
  brand TEXT NOT NULL DEFAULT'',model TEXT NOT NULL DEFAULT'',origin TEXT NOT NULL DEFAULT'',
  dimensions TEXT NOT NULL DEFAULT'',weight TEXT NOT NULL DEFAULT'',material TEXT NOT NULL DEFAULT'',
  spec1_key TEXT NOT NULL DEFAULT'',spec1_val TEXT NOT NULL DEFAULT'',
  spec2_key TEXT NOT NULL DEFAULT'',spec2_val TEXT NOT NULL DEFAULT'',
  spec3_key TEXT NOT NULL DEFAULT'',spec3_val TEXT NOT NULL DEFAULT'',
  created_at TEXT NOT NULL DEFAULT(datetime('now')),
  FOREIGN KEY(store_id) REFERENCES stores(store_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS time_slots(slot_id TEXT PRIMARY KEY,store_id TEXT NOT NULL,
  label TEXT NOT NULL,start_time TEXT NOT NULL,end_time TEXT NOT NULL,
  max_orders INTEGER NOT NULL DEFAULT 10,is_active INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY(store_id) REFERENCES stores(store_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS orders(order_id TEXT PRIMARY KEY,customer_name TEXT NOT NULL,
  store_id TEXT NOT NULL,slot_id TEXT NOT NULL,
  pickup_date TEXT NOT NULL DEFAULT(date('now')),
  status TEXT NOT NULL DEFAULT'placed' CHECK(status IN
    ('placed','preparing','ready','visited','completed','no_show','failed')),
  total_amount REAL NOT NULL DEFAULT 0,customer_confirmed INTEGER NOT NULL DEFAULT 0,
  store_confirmed INTEGER NOT NULL DEFAULT 0,visit_verified INTEGER NOT NULL DEFAULT 0,
  placed_at TEXT NOT NULL DEFAULT(datetime('now')),completed_at TEXT,
  FOREIGN KEY(store_id) REFERENCES stores(store_id),
  FOREIGN KEY(slot_id) REFERENCES time_slots(slot_id));
CREATE TABLE IF NOT EXISTS order_items(item_id TEXT PRIMARY KEY,order_id TEXT NOT NULL,
  product_id TEXT NOT NULL,product_name TEXT NOT NULL,
  quantity REAL NOT NULL CHECK(quantity>0),unit_price REAL NOT NULL CHECK(unit_price>=0),
  subtotal REAL NOT NULL,
  FOREIGN KEY(order_id) REFERENCES orders(order_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS visits(visit_id TEXT PRIMARY KEY,order_id TEXT NOT NULL UNIQUE,
  store_id TEXT NOT NULL,customer_name TEXT NOT NULL,
  qr_scanned_at TEXT NOT NULL DEFAULT(datetime('now')),
  within_slot INTEGER NOT NULL DEFAULT 1,verified INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY(order_id) REFERENCES orders(order_id));
CREATE TABLE IF NOT EXISTS reviews(review_id TEXT PRIMARY KEY,
  reviewer_type TEXT NOT NULL CHECK(reviewer_type IN('customer','owner')),
  reviewer_id TEXT NOT NULL,
  target_type TEXT NOT NULL CHECK(target_type IN('product','store','customer')),
  target_id TEXT NOT NULL,order_id TEXT,
  store_id TEXT NOT NULL DEFAULT'',
  rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
  body TEXT NOT NULL DEFAULT'',
  created_at TEXT NOT NULL DEFAULT(datetime('now')),
  updated_at TEXT NOT NULL DEFAULT(datetime('now')));
CREATE INDEX IF NOT EXISTS idx_rev_store ON reviews(store_id);
CREATE TABLE IF NOT EXISTS points(entity_id TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL CHECK(entity_type IN('customer','store')),
  total_points INTEGER NOT NULL DEFAULT 0 CHECK(total_points>=0),
  updated_at TEXT NOT NULL DEFAULT(datetime('now')));
CREATE TABLE IF NOT EXISTS transactions(txn_id TEXT PRIMARY KEY,entity_id TEXT NOT NULL,
  entity_type TEXT NOT NULL,order_id TEXT,event_type TEXT NOT NULL,
  delta INTEGER NOT NULL,note TEXT,created_at TEXT NOT NULL DEFAULT(datetime('now')));
CREATE TABLE IF NOT EXISTS store_analytics(store_id TEXT PRIMARY KEY,
  total_orders INTEGER NOT NULL DEFAULT 0,completed_orders INTEGER NOT NULL DEFAULT 0,
  total_visits INTEGER NOT NULL DEFAULT 0,no_shows INTEGER NOT NULL DEFAULT 0,
  repeat_customers INTEGER NOT NULL DEFAULT 0,total_revenue REAL NOT NULL DEFAULT 0,
  peak_slots TEXT NOT NULL DEFAULT'{}',last_updated TEXT NOT NULL DEFAULT(datetime('now')),
  FOREIGN KEY(store_id) REFERENCES stores(store_id));
CREATE TABLE IF NOT EXISTS review_ai_cache(target_id TEXT PRIMARY KEY,
  target_type TEXT NOT NULL,summary TEXT NOT NULL,
  generated_at TEXT NOT NULL DEFAULT(datetime('now')));
CREATE INDEX IF NOT EXISTS idx_ord_store   ON orders(store_id);
CREATE INDEX IF NOT EXISTS idx_ord_cust    ON orders(customer_name);
CREATE INDEX IF NOT EXISTS idx_ord_status  ON orders(status);
CREATE INDEX IF NOT EXISTS idx_ord_date    ON orders(pickup_date);
CREATE INDEX IF NOT EXISTS idx_prod_store  ON products(store_id);
CREATE INDEX IF NOT EXISTS idx_slot_store  ON time_slots(store_id);
CREATE INDEX IF NOT EXISTS idx_visit_ord   ON visits(order_id);
CREATE INDEX IF NOT EXISTS idx_txn_entity  ON transactions(entity_id);
CREATE INDEX IF NOT EXISTS idx_store_owner ON stores(owner_id);
CREATE INDEX IF NOT EXISTS idx_rev_target  ON reviews(target_type,target_id);
CREATE INDEX IF NOT EXISTS idx_rev_reviewer ON reviews(reviewer_type,reviewer_id);
CREATE TABLE IF NOT EXISTS notifications(
  notif_id    TEXT PRIMARY KEY,
  recipient_type TEXT NOT NULL CHECK(recipient_type IN('customer','owner')),
  recipient_id   TEXT NOT NULL,
  title          TEXT NOT NULL,
  body           TEXT NOT NULL,
  icon           TEXT NOT NULL DEFAULT '🔔',
  is_read        INTEGER NOT NULL DEFAULT 0,
  link           TEXT NOT NULL DEFAULT '',
  created_at     TEXT NOT NULL DEFAULT(datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_notif_recipient ON notifications(recipient_type,recipient_id,is_read);
CREATE TABLE IF NOT EXISTS fcm_tokens(
  token_id       TEXT PRIMARY KEY,
  recipient_type TEXT NOT NULL CHECK(recipient_type IN('customer','owner')),
  recipient_id   TEXT NOT NULL,
  fcm_token      TEXT NOT NULL UNIQUE,
  created_at     TEXT NOT NULL DEFAULT(datetime('now')),
  last_seen      TEXT NOT NULL DEFAULT(datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_fcm_recipient ON fcm_tokens(recipient_type,recipient_id);
-- Ledger system (isolated, safe)
CREATE TABLE IF NOT EXISTS ledger_customers(
  id TEXT PRIMARY KEY,
  store_id TEXT,
  name TEXT,
  phone TEXT
);

CREATE TABLE IF NOT EXISTS ledger_entries(
  id TEXT PRIMARY KEY,
  customer_id TEXT,
  amount REAL,
  type TEXT,
  date TEXT
);
"""

def init_db(db_path):
    conn = _direct_conn(db_path)
    conn.executescript(SCHEMA)
    _run_migrations(conn)
    conn.commit(); conn.close()

def _run_migrations(conn):
    def _has_col(tbl, col):
        return any(r["name"]==col for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall())
    migs = [
        ("products","description","TEXT NOT NULL DEFAULT''"),
        ("products","image_b64","TEXT"),("products","image_mime","TEXT NOT NULL DEFAULT'image/jpeg'"),
        ("products","brand","TEXT NOT NULL DEFAULT''"),("products","model","TEXT NOT NULL DEFAULT''"),
        ("products","origin","TEXT NOT NULL DEFAULT''"),("products","dimensions","TEXT NOT NULL DEFAULT''"),
        ("products","weight","TEXT NOT NULL DEFAULT''"),("products","material","TEXT NOT NULL DEFAULT''"),
        ("products","spec1_key","TEXT NOT NULL DEFAULT''"),("products","spec1_val","TEXT NOT NULL DEFAULT''"),
        ("products","spec2_key","TEXT NOT NULL DEFAULT''"),("products","spec2_val","TEXT NOT NULL DEFAULT''"),
        ("products","spec3_key","TEXT NOT NULL DEFAULT''"),("products","spec3_val","TEXT NOT NULL DEFAULT''"),
        ("products","created_at","TEXT NOT NULL DEFAULT(datetime('now'))"),
        ("orders","pickup_date","TEXT NOT NULL DEFAULT(date('now'))"),
        ("owners","is_active","INTEGER NOT NULL DEFAULT 1"),
        ("stores","is_approved","INTEGER NOT NULL DEFAULT 1"),
        ("stores","description","TEXT NOT NULL DEFAULT''"),
        ("stores","image_b64","TEXT"),
        ("stores","image_mime","TEXT NOT NULL DEFAULT'image/jpeg'"),
        ("transactions","note","TEXT"),
        ("reviews","store_id","TEXT NOT NULL DEFAULT''"),
        ("products","stock","INTEGER NOT NULL DEFAULT 0"),
        ("stores","latitude","REAL"),
        ("stores","longitude","REAL"),
    ]
    # Ensure notifications table exists (cannot ALTER, it's a new table)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS fcm_tokens(
      token_id TEXT PRIMARY KEY,
      recipient_type TEXT NOT NULL,
      recipient_id   TEXT NOT NULL,
      fcm_token      TEXT NOT NULL UNIQUE,
      created_at     TEXT NOT NULL DEFAULT(datetime('now')),
      last_seen      TEXT NOT NULL DEFAULT(datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_fcm_recipient ON fcm_tokens(recipient_type,recipient_id);
    """)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS notifications(
      notif_id TEXT PRIMARY KEY,
      recipient_type TEXT NOT NULL,
      recipient_id   TEXT NOT NULL,
      title          TEXT NOT NULL,
      body           TEXT NOT NULL,
      icon           TEXT NOT NULL DEFAULT '🔔',
      is_read        INTEGER NOT NULL DEFAULT 0,
      link           TEXT NOT NULL DEFAULT '',
      created_at     TEXT NOT NULL DEFAULT(datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_notif_recipient
      ON notifications(recipient_type,recipient_id,is_read);
    """)
    for tbl, col, defn in migs:
        if not _has_col(tbl, col):
            try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {defn}")
            except Exception: pass

DEFAULT_SLOTS = [
    ("8:00 AM - 9:00 AM","08:00","09:00"),("9:00 AM - 10:00 AM","09:00","10:00"),
    ("10:00 AM - 11:00 AM","10:00","11:00"),("11:00 AM - 12:00 PM","11:00","12:00"),
    ("12:00 PM - 1:00 PM","12:00","13:00"),("2:00 PM - 3:00 PM","14:00","15:00"),
    ("3:00 PM - 4:00 PM","15:00","16:00"),("4:00 PM - 5:00 PM","16:00","17:00"),
    ("5:00 PM - 6:00 PM","17:00","18:00"),
]
STORE_CATEGORIES = ["Grocery","Pharmacy","Hardware","Vegetables","Dairy",
                    "Electronics","Clothing","Bakery","General"]
PRODUCT_UNITS = ["piece","kg","litre","pack","box","dozen","bunch","strip","bottle","metre"]

def _insert_default_slots(conn, sid):
    for i,(lbl,st,et) in enumerate(DEFAULT_SLOTS):
        conn.execute("INSERT OR IGNORE INTO time_slots "
                     "(slot_id,store_id,label,start_time,end_time,max_orders) VALUES(?,?,?,?,?,?)",
                     (f"slot_{sid}_{i}",sid,lbl,st,et,10))

def seed_demo_data(db_path):
    conn = _direct_conn(db_path)
    if not conn.execute("SELECT 1 FROM admins LIMIT 1").fetchone():
        conn.execute("INSERT INTO admins(admin_id,username,password) VALUES(?,?,?)",
                     ("admin_001","admin",generate_password_hash(
                         os.environ.get("ADMIN_PASSWORD","admin123"))))
    if not conn.execute("SELECT 1 FROM owners LIMIT 1").fetchone():
        oid = "owner_demo"
        conn.execute("INSERT INTO owners(owner_id,username,password,full_name,phone) VALUES(?,?,?,?,?)",
                     (oid,"demo",generate_password_hash("demo123"),"Demo Owner","9999999999"))
        sid = str(uuid.uuid4())
        qr  = _make_qr_payload(sid)
        conn.execute("INSERT INTO stores(store_id,owner_id,name,owner_name,address,category,qr_code,points)"
                     " VALUES(?,?,?,?,?,?,?,?)",
                     (sid,oid,"Sharma General Store","Demo Owner","12 MG Road Kolkata","Grocery",qr,50))
        conn.execute("INSERT OR IGNORE INTO store_analytics(store_id) VALUES(?)",(sid,))
        conn.execute("INSERT OR IGNORE INTO points(entity_id,entity_type,total_points) VALUES(?,?,?)",
                     (sid,"store",50))
        for pid,nm,price,unit,desc,brand in [
            ("pd1","Basmati Rice",85.0,"kg","Premium aged long-grain basmati. Perfect aroma.","India Gate"),
            ("pd2","Mustard Oil",180.0,"litre","Cold-pressed kachi ghani. Rich omega-3.","Patanjali"),
            ("pd3","Sugar",48.0,"kg","Refined free-flowing crystals. ISI certified.","Uttam"),
            ("pd4","Wheat Atta",55.0,"kg","Chakki-fresh whole wheat. Stone-ground.","Aashirvaad"),
            ("pd5","Toor Dal",130.0,"kg","Premium pigeon pea lentils. Washed and polished.","Local Farm"),
        ]:
            conn.execute("INSERT OR IGNORE INTO products"
                         "(product_id,store_id,name,price,unit,description,brand) VALUES(?,?,?,?,?,?,?)",
                         (pid,sid,nm,price,unit,desc,brand))
        _insert_default_slots(conn,sid)
        # Seed some demo reviews so the rating shows on the store card
        import uuid as _uuid
        demo_reviews = [
            (str(_uuid.uuid4()), "customer", "demo_customer1", "store", sid, sid, 5,
             "Excellent service! Always fresh stock and very polite owner."),
            (str(_uuid.uuid4()), "customer", "demo_customer2", "store", sid, sid, 4,
             "Good quality products, reasonable prices. Highly recommend."),
            (str(_uuid.uuid4()), "customer", "demo_customer3", "store", sid, sid, 5,
             "Best grocery store in the area. Fresh vegetables every day!"),
        ]
        for rv in demo_reviews:
            conn.execute(
                "INSERT OR IGNORE INTO reviews(review_id,reviewer_type,reviewer_id,"
                "target_type,target_id,store_id,rating,body) VALUES(?,?,?,?,?,?,?,?)",
                rv)
    conn.commit(); conn.close()

# ── RATE LIMITER ──────────────────────────────────────────────────────────────
_rate_store: dict = defaultdict(list)
def _rate_check(key, limit, window):
    now = time.time()
    _rate_store[key] = [t for t in _rate_store[key] if now-t<window]
    if len(_rate_store[key]) >= limit: return False
    _rate_store[key].append(now); return True

def rate_limit(limit_key):
    def dec(f):
        @wraps(f)
        def wrapped(*a,**kw):
            from flask import current_app
            cfg = current_app.config
            if not cfg.get("RATE_LIMIT_ENABLED",True): return f(*a,**kw)
            if not _rate_check(f"{limit_key}:{request.remote_addr or'unk'}",
                               cfg.get(f"RATE_LIMIT_{limit_key}",20),
                               cfg.get("RATE_LIMIT_WINDOW",3600)):
                current_app.logger.warning("Rate limited: %s %s",limit_key,request.remote_addr)
                abort(429)
            return f(*a,**kw)
        return wrapped
    return dec

# ── VALIDATION ────────────────────────────────────────────────────────────────
_USERNAME_RE = re.compile(r'^[a-z0-9_]{3,40}$')
def sanitize(v, n=200): return html.escape(str(v or"").strip()[:n])
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (math.sin(dlat/2) ** 2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon/2) ** 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

def geocode_location(query):
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": query, "format": "json", "limit": 1}
        headers = {"User-Agent": "ApnaDukaan/1.0 (store-locator; contact@apnadukaan.in)"}
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        import logging; logging.getLogger(__name__).warning("GEOCODING ERROR for %r: %s", query, e)
    return None, None


def validate_username(u):
    if not _USERNAME_RE.match(u): return "Username: 3–40 chars, lowercase/numbers/underscores only."
def validate_price(v):
    try:
        p=float(v)
        if p<0: return None,"Price cannot be negative."
        if p>1e5: return None,"Price exceeds maximum."
        return round(p,2),None
    except: return None,"Invalid price."
def validate_quantity(v):
    try:
        q=float(v)
        if q<=0: return None,"Quantity must be positive."
        if q>100: return None,"Max quantity is 100."
        return q,None
    except: return None,"Invalid quantity."
def validate_rating(v):
    try:
        r=int(v)
        if 1<=r<=5: return r,None
        return None,"Rating must be 1–5."
    except: return None,"Invalid rating."

def process_image(fs):
    from flask import current_app
    if not fs or not fs.filename: return None,None
    mime = fs.mimetype or "image/jpeg"
    if mime not in ("image/jpeg","image/png","image/webp","image/gif"):
        raise ValueError("Only JPEG, PNG, WebP or GIF accepted.")
    data = fs.read()
    if len(data) > current_app.config["MAX_IMAGE_BYTES"]:
        raise ValueError("Image must be under 2 MB.")
    return base64.b64encode(data).decode(), mime

# ── POINTS ────────────────────────────────────────────────────────────────────
POINTS_RULES = {
    "order_placed":{"customer":2,"store":0}, "order_complete":{"customer":10,"store":10},
    "visit_verified_ontime":{"customer":5,"store":5}, "no_show":{"customer":-15,"store":0},
    "store_failed":{"customer":0,"store":-20}, "late_arrival":{"customer":-5,"store":0},
    "store_not_ready":{"customer":0,"store":-10}, "review_left":{"customer":3,"store":2},
}
POINTS_NOTIF_MSGS = {
    "order_placed":          ("🛒", "Order Placed",           "+{d} pts for placing an order!"),
    "order_complete":        ("✅", "Order Completed",         "+{d} pts — order fully completed!"),
    "visit_verified_ontime":("⏰", "On-Time Visit",           "+{d} pts for arriving on time!"),
    "late_arrival":         ("⚠️", "Late Arrival",            "{d} pts — you arrived outside your slot."),
    "no_show":              ("❌", "No-Show Penalty",          "{d} pts — order marked as no-show."),
    "store_failed":         ("❌", "Store Penalty",            "{d} pts — store failed to fulfil an order."),
    "store_not_ready":      ("⚠️", "Store Not Ready Penalty",  "{d} pts — order was not ready on time."),
    "review_left":          ("✍️", "Review Bonus",             "+{d} pts for leaving a review!"),
}

def award_points(eid, etype, event, order_id=None, note=None):
    delta = POINTS_RULES.get(event,{}).get(etype,0)
    if delta==0: return
    conn = get_db()
    conn.execute("""INSERT INTO points(entity_id,entity_type,total_points,updated_at)
       VALUES(?,?,MAX(0,?),datetime('now'))
       ON CONFLICT(entity_id) DO UPDATE SET total_points=MAX(0,total_points+?),updated_at=datetime('now')""",
       (eid,etype,delta,delta))
    conn.execute("INSERT INTO transactions(txn_id,entity_id,entity_type,order_id,event_type,delta,note)"
                 " VALUES(?,?,?,?,?,?,?)",
                 (str(uuid.uuid4()),eid,etype,order_id,event,delta,note))
    conn.commit()
    # Push notification
    msg_tmpl = POINTS_NOTIF_MSGS.get(event)
    if msg_tmpl:
        icon, title, body_tmpl = msg_tmpl
        d_str = (f"+{delta}" if delta > 0 else str(delta))
        body  = body_tmpl.format(d=d_str)
        link  = "/my-points" if etype == "customer" else "/owner/analytics"
        push_notification(etype, eid, title, body, icon, link)

def push_notification(recipient_type, recipient_id, title, body, icon='🔔', link=''):
    """Create an in-app notification for a customer or owner."""
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO notifications(notif_id,recipient_type,recipient_id,title,body,icon,link) "
            "VALUES(?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), recipient_type, recipient_id, title, body, icon, link))
        conn.commit()
    except Exception:
        pass  # notifications are best-effort

# ── FCM PUSH ─────────────────────────────────────────────────────────────────
# Maps event names to (title_template, body_template, link_template).
# {name} = customer name, {store} = store name, {id} = order_id
FCM_EVENTS = {
    "order_placed":          ("🛒 New Order",          "{name} placed an order · ₹{amount}",       "/owner/orders"),
    "order_ready":           ("✅ Order Ready!",        "Your order at {store} is ready for pickup!", "/my-orders"),
    "order_complete":        ("🎉 Order Completed",     "Order at {store} completed. +10 pts!",       "/my-orders"),
    "order_visited":         ("📦 Customer Arrived",   "{name} just scanned in at your store.",      "/owner/orders"),
    "order_handover_owner":  ("🤝 Handover Confirmed", "Store confirmed handover for order {id}.",   "/my-orders"),
    "order_handover_cust":   ("🤝 Handover Confirmed", "{name} confirmed receipt of order {id}.",    "/owner/orders"),
    "no_show":               ("❌ No-Show",             "Order {id} marked as no-show.",              "/my-orders"),
    "order_failed":          ("❌ Order Failed",        "Order {id} could not be fulfilled.",         "/my-orders"),
}

def push_fcm(recipient_type, recipient_id, title, body, link='/'):
    """Send a Firebase Cloud Messaging push to all tokens registered for this recipient."""
    from flask import current_app
    server_key = current_app.config.get("FCM_SERVER_KEY", "")
    project_id = current_app.config.get("FCM_PROJECT_ID", "")
    if not server_key or not project_id:
        return  # FCM not configured — silently skip

    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT fcm_token FROM fcm_tokens WHERE recipient_type=? AND recipient_id=?",
            (recipient_type, recipient_id)
        ).fetchall()
        if not rows:
            return

        import urllib.request as _ur
        for row in rows:
            token = row["fcm_token"]
            payload = json.dumps({
                "message": {
                    "token": token,
                    "notification": {"title": title, "body": body},
                    "data": {"title": title, "body": body, "link": link},
                    "android": {
                        "notification": {
                            "click_action": "FLUTTER_NOTIFICATION_CLICK",
                            "sound": "default",
                            "priority": "high",
                        }
                    },
                    "webpush": {
                        "fcm_options": {"link": link},
                        "notification": {"requireInteraction": False, "vibrate": [200, 100, 200]},
                    },
                }
            }).encode()
            req = _ur.Request(
                f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {_get_fcm_access_token(server_key)}",
                },
                method="POST",
            )
            try:
                with _ur.urlopen(req, timeout=5) as resp:
                    pass  # success
            except Exception as e:
                current_app.logger.warning("FCM send failed for token %s: %s", token[:20], e)
                # Remove invalid/expired tokens (401/404 from FCM)
                if hasattr(e, 'code') and e.code in (400, 404):
                    conn.execute("DELETE FROM fcm_tokens WHERE fcm_token=?", (token,))
                    conn.commit()
    except Exception as e:
        current_app.logger.warning("push_fcm error: %s", e)


def _get_fcm_access_token(service_account_json_str):
    """Exchange a Firebase service-account JSON string for a short-lived OAuth2 bearer token."""
    import time, base64 as _b64, hmac as _hmac, hashlib as _hl
    try:
        sa = json.loads(service_account_json_str)
        now = int(time.time())
        header  = _b64.urlsafe_b64encode(json.dumps({"alg":"RS256","typ":"JWT"}).encode()).rstrip(b'=')
        payload = _b64.urlsafe_b64encode(json.dumps({
            "iss": sa["client_email"],
            "scope": "https://www.googleapis.com/auth/firebase.messaging",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now, "exp": now + 3600,
        }).encode()).rstrip(b'=')
        # Sign with RSA private key using cryptography library if available,
        # otherwise fall back to storing the Legacy Server Key directly.
        try:
            from cryptography.hazmat.primitives import serialization, hashes
            from cryptography.hazmat.primitives.asymmetric import padding
            from cryptography.hazmat.backends import default_backend
            import urllib.request as _ur, urllib.parse as _up
            private_key = serialization.load_pem_private_key(
                sa["private_key"].encode(), password=None, backend=default_backend())
            sig_input = header + b'.' + payload
            sig = _b64.urlsafe_b64encode(
                private_key.sign(sig_input, padding.PKCS1v15(), hashes.SHA256())).rstrip(b'=')
            jwt_token = (sig_input + b'.' + sig).decode()
            req = _ur.Request("https://oauth2.googleapis.com/token",
                data=_up.urlencode({"grant_type":"urn:ietf:params:oauth:grant-type:jwt-bearer",
                                    "assertion": jwt_token}).encode(),
                headers={"Content-Type":"application/x-www-form-urlencoded"}, method="POST")
            with _ur.urlopen(req, timeout=10) as r:
                return json.loads(r.read())["access_token"]
        except ImportError:
            # cryptography not installed — caller should use Legacy HTTP API with server key directly
            return service_account_json_str  # will be used as Bearer token (legacy key fallback)
    except Exception:
        return service_account_json_str


def push_fcm_event(event, recipient_type, recipient_id, **kwargs):
    """Fire a named FCM push event with template substitution."""
    tmpl = FCM_EVENTS.get(event)
    if not tmpl:
        return
    title_t, body_t, link_t = tmpl
    try:
        title = title_t.format(**kwargs)
        body  = body_t.format(**kwargs)
        link  = kwargs.get("link", link_t)
    except KeyError:
        title, body, link = title_t, body_t, link_t
    push_fcm(recipient_type, recipient_id, title, body, link)


def get_unread_count(recipient_type, recipient_id):
    try:
        r = get_db().execute(
            "SELECT COUNT(*) FROM notifications WHERE recipient_type=? AND recipient_id=? AND is_read=0",
            (recipient_type, recipient_id)).fetchone()
        return r[0] if r else 0
    except Exception:
        return 0

def get_points(eid):
    r = get_db().execute("SELECT total_points FROM points WHERE entity_id=?",(eid,)).fetchone()
    return int(r["total_points"]) if r else 0

def get_tier(pts):
    if pts>=200: return "Star","⭐"
    if pts>=50:  return "Trusted","✅"
    return "New","🆕"

def update_analytics(sid):
    conn=get_db(); c=conn.cursor()
    row=c.execute("""SELECT COUNT(*) t,SUM(status='completed') comp,SUM(status='no_show') ns,
        SUM(CASE WHEN status='completed' THEN total_amount ELSE 0 END) rev FROM orders WHERE store_id=?""",
        (sid,)).fetchone()
    vis=c.execute("SELECT COUNT(*) FROM visits WHERE store_id=?",(sid,)).fetchone()[0]
    rep=c.execute("""SELECT COUNT(DISTINCT customer_name) FROM(
        SELECT customer_name FROM orders WHERE store_id=? GROUP BY customer_name HAVING COUNT(*)>=2)""",
        (sid,)).fetchone()[0]
    sr =c.execute("""SELECT ts.label,COUNT(*) cnt FROM orders o
        JOIN time_slots ts ON o.slot_id=ts.slot_id WHERE o.store_id=?
        GROUP BY ts.label ORDER BY cnt DESC LIMIT 5""",(sid,)).fetchall()
    peak=json.dumps({r["label"]:r["cnt"] for r in sr})
    c.execute("""INSERT INTO store_analytics
        (store_id,total_orders,completed_orders,total_visits,no_shows,repeat_customers,total_revenue,peak_slots,last_updated)
        VALUES(?,?,?,?,?,?,?,?,datetime('now'))
        ON CONFLICT(store_id) DO UPDATE SET total_orders=excluded.total_orders,
        completed_orders=excluded.completed_orders,total_visits=excluded.total_visits,
        no_shows=excluded.no_shows,repeat_customers=excluded.repeat_customers,
        total_revenue=excluded.total_revenue,peak_slots=excluded.peak_slots,last_updated=datetime('now')""",
        (sid,row["t"] or 0,row["comp"] or 0,vis,row["ns"] or 0,rep,row["rev"] or 0,peak))
    conn.commit()

# for getting distance
def get_coordinates(address):
    geolocator = Nominatim(user_agent="apna_dukaan")

    location = geolocator.geocode(address)

    if location:
        return location.latitude, location.longitude
    return None, None

# ── REVIEW HELPERS ────────────────────────────────────────────────────────────
def get_avg_rating(ttype, tid):
    r=get_db().execute("SELECT AVG(rating) avg,COUNT(*) cnt FROM reviews WHERE target_type=? AND target_id=?",
                       (ttype,tid)).fetchone()
    if not r or not r["avg"]: return None,0
    return round(r["avg"],1), r["cnt"]

def stars_html(rating):
    if not rating: return ""
    f=int(rating); h=1 if rating-f>=0.5 else 0; e=5-f-h
    return "★"*f+("½" if h else "")+"☆"*e

def get_ai_review_summary(ttype, tid):
    from flask import current_app
    api_key = current_app.config.get("ANTHROPIC_API_KEY","")
    if not api_key: return None
    conn = get_db()
    cached = conn.execute("SELECT summary,generated_at FROM review_ai_cache WHERE target_id=? AND target_type=?",
                          (tid,ttype)).fetchone()
    if cached:
        age = (datetime.now()-datetime.fromisoformat(cached["generated_at"])).total_seconds()/3600
        if age < 24: return cached["summary"]
    reviews = conn.execute("SELECT rating,body FROM reviews WHERE target_type=? AND target_id=? AND body!='' "
                           "ORDER BY created_at DESC LIMIT 20",(ttype,tid)).fetchall()
    if len(reviews) < 2: return None
    txt = "\n".join(f"- {r['rating']} stars: {r['body']}" for r in reviews)
    try:
        import urllib.request
        payload = json.dumps({"model":"claude-haiku-4-5-20251001","max_tokens":200,
            "messages":[{"role":"user","content":
                f"Summarize these customer reviews for a {'product' if ttype=='product' else 'store'} "
                f"in 2-3 concise sentences. Highlight what customers love and any concerns.\n\nReviews:\n{txt}"}]
        }).encode()
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",data=payload,
            headers={"Content-Type":"application/json","x-api-key":api_key,
                     "anthropic-version":"2023-06-01"},method="POST")
        with urllib.request.urlopen(req,timeout=8) as resp:
            data    = json.loads(resp.read().decode())
            summary = data["content"][0]["text"].strip()
        conn.execute("""INSERT INTO review_ai_cache(target_id,target_type,summary,generated_at)
            VALUES(?,?,?,datetime('now'))
            ON CONFLICT(target_id) DO UPDATE SET summary=excluded.summary,generated_at=datetime('now')""",
            (tid,ttype,summary)); conn.commit()
        return summary
    except Exception as e:
        current_app.logger.warning("AI summary failed %s %s: %s",ttype,tid,e)
        return None

# ── QR / UTILS ────────────────────────────────────────────────────────────────
def _make_qr_payload(sid):
    return f"STORE:{sid}:{hashlib.sha256(f'apnadukaan-{sid}'.encode()).hexdigest()[:12]}"

def generate_qr_b64(data):
    if not QR_AVAILABLE: return None
    try:
        qr=qrcode.QRCode(version=2,box_size=6,border=2); qr.add_data(data); qr.make(fit=True)
        img=qr.make_image(fill_color="#1a1a1a",back_color="white")
        buf=io.BytesIO(); img.save(buf,format="PNG"); return base64.b64encode(buf.getvalue()).decode()
    except: return None

def pickup_dates(days=7):
    today=date.today()
    return [((today+timedelta(days=i)).isoformat(),(today+timedelta(days=i)).strftime("%a, %d %b"))
            for i in range(days)]

# ── GUARDS ────────────────────────────────────────────────────────────────────
def owner_required(f):
    @wraps(f)
    def d(*a,**kw):
        if not session.get("owner_id"):
            flash("Please log in to access your store dashboard.","error")
            return redirect(url_for("owner_login"))
        return f(*a,**kw)
    return d

def customer_required(f):
    @wraps(f)
    def d(*a,**kw):
        if not session.get("customer_name"): return redirect(url_for("customer_home"))
        return f(*a,**kw)
    return d

def admin_required(f):
    @wraps(f)
    def d(*a,**kw):
        if not session.get("admin_id"): return redirect(url_for("admin_login"))
        return f(*a,**kw)
    return d

# ── SECURITY HEADERS ──────────────────────────────────────────────────────────
def _register_security_headers(app):
    @app.after_request
    def add_headers(response):
        h = response.headers
        h["X-Content-Type-Options"]="nosniff"; h["X-Frame-Options"]="SAMEORIGIN"
        h["X-XSS-Protection"]="1; mode=block"; h["Referrer-Policy"]="strict-origin-when-cross-origin"
        h["Permissions-Policy"]="geolocation=(self),microphone=(),camera=()"
        if app.config.get("SESSION_COOKIE_SECURE"):
            h["Strict-Transport-Security"]="max-age=31536000; includeSubDomains"
        if request.path.startswith(("/owner/","/my-","/control-panel/")):
            h["Cache-Control"]="no-store,no-cache,must-revalidate,private"
        return response

# ── ERROR HANDLERS ────────────────────────────────────────────────────────────
def _register_error_handlers(app):
    for code,tpl in [(400,"400"),(403,"403"),(404,"404"),(429,"429"),(500,"500")]:
        def _make(t,c):
            def h(e):
                if c==500: app.logger.exception("Internal server error")
                return render_template(f"errors/{t}.html"),c
            h.__name__=f"error_{c}"; return h
        app.register_error_handler(code,_make(tpl,code))

# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════
def _register_routes(app):
    @app.route("/search")
    @customer_required
    def search_page():
        return render_template("search.html")
    @app.before_request
    def enforce_roles():
        app.logger.info("%s %s ip=%s u=%s role=%s",
            request.method,request.path,request.remote_addr,
            session.get("customer_name") or session.get("owner_name") or "anon",
            session.get("role","-"))
        role=session.get("role"); path=request.path
        if role=="customer" and path.startswith("/owner/"):
            flash("You are signed in as a customer. Log out first to access owner features.","error")
            return redirect(url_for("store_list"))
        CUST_PATHS=("/stores","/store/","/product/","/my-orders","/my-points","/scan",
                    "/verify","/confirm-receipt","/checkout","/order/place","/cart","/review/submit")
        if role=="owner" and any(path.startswith(p) for p in CUST_PATHS):
            flash("You are signed in as a store owner. Log out first to shop as a customer.","error")
            return redirect(url_for("owner_dashboard"))

    # ── SHARED ────────────────────────────────────────────────────────────────
    @app.route("/")
    def index():
        r=session.get("role")
        if r=="customer": return redirect(url_for("store_list"))
        if r=="owner":    return redirect(url_for("owner_dashboard"))
        if r=="admin":    return redirect(url_for("admin_dashboard"))
        return render_template("index.html")

    @app.route("/health")
    def health():
        try: get_db().execute("SELECT 1").fetchone(); return jsonify({"status":"ok","db":"ok"}),200
        except Exception as e: return jsonify({"status":"error","db":str(e)}),500

    # ── CUSTOMER AUTH ─────────────────────────────────────────────────────────
    @app.route("/customer",methods=["GET","POST"])
    def customer_home():
        r=session.get("role")
        if r=="customer": return redirect(url_for("store_list"))
        if r=="owner":    return redirect(url_for("owner_dashboard"))
        if r=="admin":    return redirect(url_for("admin_dashboard"))
        if request.method=="POST":
            name=sanitize(request.form.get("name",""),app.config["MAX_NAME_LEN"])
            if not name: flash("Please enter your name.","error")
            else:
                session["customer_name"]=name; session["role"]="customer"; session.permanent=True
                return redirect(url_for("store_list"))
        return render_template("customer_login.html")

    @app.route("/customer/logout")
    def customer_logout(): session.clear(); return redirect(url_for("index"))

    # ── STORES ────────────────────────────────────────────────────────────────
    @app.route("/stores")
    @customer_required
    def store_list():
        stores=get_db().execute(
            "SELECT * FROM stores WHERE is_open=1 AND is_approved=1 ORDER BY points DESC,name"
        ).fetchall()
        data=[]
        for s in stores:
            avg,cnt=get_avg_rating("store",s["store_id"])
            pts=get_points(s["store_id"]); tier,icon=get_tier(pts)
            prod_count=get_db().execute(
                "SELECT COUNT(*) FROM products WHERE store_id=? AND available=1",(s["store_id"],)
            ).fetchone()[0]
            data.append({"store":s,"points":pts,"tier":tier,"icon":icon,
                         "avg_rating":avg,"rating_count":cnt,"prod_count":prod_count,
                         "lat":s["latitude"],"lng":s["longitude"],"distance":None})
        return render_template("store_list.html",stores=data,customer_name=session["customer_name"])

    @app.route("/api/nearby-stores")
    @customer_required
    def api_nearby_stores():
        """Return stores within radius_m metres of the given lat/lng.
        Query params: lat, lng, radius (default 500 m).
        Returns JSON list sorted by distance ascending.
        """
        try:
            user_lat = float(request.args.get("lat", ""))
            user_lng = float(request.args.get("lng", ""))
        except (TypeError, ValueError):
            return jsonify({"error": "lat and lng are required numeric parameters."}), 400

        try:
            radius_m = float(request.args.get("radius", 500))
            if radius_m <= 0 or radius_m > 50000:
                radius_m = 500
        except (TypeError, ValueError):
            radius_m = 500

        radius_km = radius_m / 1000.0
        stores = get_db().execute(
            "SELECT * FROM stores WHERE is_open=1 AND is_approved=1 "
            "AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchall()

        nearby = []
        for s in stores:
            dist_km = calculate_distance(user_lat, user_lng, s["latitude"], s["longitude"])
            if dist_km <= radius_km:
                avg, cnt = get_avg_rating("store", s["store_id"])
                pts = get_points(s["store_id"])
                tier, icon = get_tier(pts)
                prod_count = get_db().execute(
                    "SELECT COUNT(*) FROM products WHERE store_id=? AND available=1",
                    (s["store_id"],)
                ).fetchone()[0]
                nearby.append({
                    "store_id":     s["store_id"],
                    "name":         s["name"],
                    "owner_name":   s["owner_name"],
                    "address":      s["address"],
                    "category":     s["category"],
                    "is_open":      bool(s["is_open"]),
                    "points":       pts,
                    "tier":         tier,
                    "tier_icon":    icon,
                    "avg_rating":   avg,
                    "rating_count": cnt,
                    "prod_count":   prod_count,
                    "latitude":     s["latitude"],
                    "longitude":    s["longitude"],
                    "distance_m":   round(dist_km * 1000, 1),
                    "image_b64":    s["image_b64"],
                    "image_mime":   s["image_mime"],
                })

        nearby.sort(key=lambda x: x["distance_m"])
        return jsonify({"radius_m": radius_m, "count": len(nearby), "stores": nearby})

    @app.route("/store/<store_id>")
    @customer_required
    def store_detail(store_id):
        conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=? AND is_approved=1",(store_id,)).fetchone()
        if not store: abort(404)
        products=conn.execute(
            "SELECT * FROM products WHERE store_id=? AND available=1 ORDER BY category,name",(store_id,)
        ).fetchall()
        cats={}
        for p in products:
            avg,cnt=get_avg_rating("product",p["product_id"])
            cats.setdefault(p["category"],[]).append({**dict(p),"avg_rating":avg,"rating_count":cnt})
        pts=get_points(store_id); tier,icon=get_tier(pts)
        avg_r,r_cnt=get_avg_rating("store",store_id)
        reviews=conn.execute("SELECT * FROM reviews WHERE target_type='store' AND target_id=? "
                             "ORDER BY created_at DESC LIMIT 10",(store_id,)).fetchall()
        ai_summary=get_ai_review_summary("store",store_id)
        cname=session["customer_name"]
        existing_rev=conn.execute("SELECT * FROM reviews WHERE reviewer_type='customer' AND reviewer_id=? "
                                  "AND target_type='store' AND target_id=?",(cname,store_id)).fetchone()
        has_order=conn.execute("SELECT 1 FROM orders WHERE customer_name=? AND store_id=? "
                               "AND status='completed' LIMIT 1",(cname,store_id)).fetchone()
        return render_template("store_detail.html",store=store,categories=cats,
                               store_points=pts,tier=tier,tier_icon=icon,
                               avg_rating=avg_r,rating_count=r_cnt,reviews=reviews,
                               ai_summary=ai_summary,existing_store_review=existing_rev,
                               has_completed_order=has_order,cart=session.get("cart",{}),
                               customer_name=cname)

    # ── PRODUCT PAGE ──────────────────────────────────────────────────────────
    @app.route("/product/<product_id>")
    @customer_required
    def product_detail(product_id):
        conn=get_db()
        product=conn.execute(
            "SELECT p.*,s.name AS store_name,s.store_id,s.is_open,s.is_approved "
            "FROM products p JOIN stores s ON p.store_id=s.store_id WHERE p.product_id=?",(product_id,)
        ).fetchone()
        if not product or not product["is_approved"]: abort(404)
        avg_r,r_cnt=get_avg_rating("product",product_id)
        reviews=conn.execute("SELECT * FROM reviews WHERE target_type='product' AND target_id=? "
                             "ORDER BY created_at DESC",(product_id,)).fetchall()
        ai_summary=get_ai_review_summary("product",product_id)
        freq=conn.execute("""SELECT p2.product_id,p2.name,p2.price,p2.unit,p2.image_b64,p2.image_mime,
            COUNT(*) c FROM order_items oi1
            JOIN order_items oi2 ON oi1.order_id=oi2.order_id AND oi2.product_id!=oi1.product_id
            JOIN products p2 ON oi2.product_id=p2.product_id
            WHERE oi1.product_id=? AND p2.available=1
            GROUP BY p2.product_id ORDER BY c DESC LIMIT 4""",(product_id,)).fetchall()
        also=conn.execute(
            "SELECT * FROM products WHERE store_id=? AND product_id!=? AND available=1 AND category=? "
            "ORDER BY ABS(price-?) LIMIT 4",
            (product["store_id"],product_id,product["category"],product["price"])
        ).fetchall()
        cname=session["customer_name"]
        existing_rev=conn.execute("SELECT * FROM reviews WHERE reviewer_type='customer' AND reviewer_id=? "
                                  "AND target_type='product' AND target_id=?",(cname,product_id)).fetchone()
        has_bought=conn.execute("""SELECT 1 FROM order_items oi JOIN orders o ON oi.order_id=o.order_id
            WHERE oi.product_id=? AND o.customer_name=? AND o.status='completed' LIMIT 1""",
            (product_id,cname)).fetchone()
        return render_template("product_detail.html",product=product,avg_rating=avg_r,
                               rating_count=r_cnt,reviews=reviews,ai_summary=ai_summary,
                               freq_together=freq,also_browsed=also,existing_review=existing_rev,
                               has_purchased=has_bought,cart=session.get("cart",{}),customer_name=cname)
    
    @app.route('/fix_coordinates')
    def fix_coordinates():
        import time

        conn = get_db()
        stores = conn.execute("SELECT store_id, address FROM stores").fetchall()
        updated = 0

        for s in stores:
            address = s["address"]
            if not address:
                print("❌ Missing address for store_id:", s["store_id"])
                continue

            full_address = address + ", Kolkata"
            lat, lng = geocode_location(full_address)

            print(f"Processing: {full_address} -> {lat}, {lng}")

            if lat and lng:
                conn.execute(
                    "UPDATE stores SET latitude=?, longitude=? WHERE store_id=?",
                    (lat, lng, s["store_id"])
                )
                updated += 1
            else:
                print(f"⚠️ Failed geocoding: {full_address}")

            time.sleep(1)  # prevent rate-limit

        conn.commit()
        return f"✅ Updated {updated} stores"
    
    @app.route("/search_product", methods=['GET', 'POST'])
    @customer_required
    def search_product():
        q = sanitize(request.args.get("q",""),100).lower()
        location_query = request.form.get("address", "")

        if location_query:
            location_query += ", Kolkata"
            user_lat, user_lng = geocode_location(location_query)
        else:
            user_lat, user_lng = None, None

        conn = get_db()

        rows = conn.execute("""
            SELECT p.product_id, p.name, p.price, p.store_id,
                s.name as store_name, s.latitude, s.longitude
            FROM products p
            JOIN stores s ON p.store_id = s.store_id
            WHERE LOWER(p.name) LIKE ?
            AND p.available=1 AND s.is_open=1
        """, (f"%{q}%",)).fetchall()

        results = []
        for r in rows:
            lat = r["latitude"]
            lng = r["longitude"]
            if lat is None or lng is None or user_lat is None or user_lng is None:
                dist = None
                print("Not able to calc dist")
            else:
                dist = calculate_distance(user_lat, user_lng, lat, lng)

            results.append({
                "product_id": r["product_id"],
                "product": r["name"],
                "price": r["price"],
                "store": r["store_name"],
                "store_id": r["store_id"],
                "distance": round(dist, 2) if dist is not None else None,
                "lat": lat,
                "lng": lng
            })

        results.sort(key=lambda x: (x["price"] if x["distance"] is None else x["distance"],
                                    x["price"]))
        return render_template("search.html", results=results, query=q)

    # ── REVIEWS ───────────────────────────────────────────────────────────────
    @app.route("/review/submit",methods=["POST"])
    @customer_required
    def submit_review():
        ttype=request.form.get("target_type","")
        tid=sanitize(request.form.get("target_id",""),60)
        rating,err=validate_rating(request.form.get("rating",""))
        body=sanitize(request.form.get("body",""),1000)
        order_id=sanitize(request.form.get("order_id",""),20) or None
        back=request.form.get("redirect_to",url_for("store_list"))
        if ttype not in ("product","store"): flash("Invalid review target.","error"); return redirect(back)
        if err: flash(err,"error"); return redirect(back)
        cname=session["customer_name"]; conn=get_db()
        if ttype=="product":
            ok=conn.execute("""SELECT 1 FROM order_items oi JOIN orders o ON oi.order_id=o.order_id
                WHERE oi.product_id=? AND o.customer_name=? AND o.status='completed' LIMIT 1""",
                (tid,cname)).fetchone()
        else:
            ok=conn.execute("SELECT 1 FROM orders WHERE store_id=? AND customer_name=? "
                            "AND status='completed' LIMIT 1",(tid,cname)).fetchone()
        if not ok: flash("You can only review after completing an order.","error"); return redirect(back)
        existing=conn.execute("SELECT review_id FROM reviews WHERE reviewer_type='customer' "
                              "AND reviewer_id=? AND target_type=? AND target_id=?",
                              (cname,ttype,tid)).fetchone()
        if existing:
            conn.execute("UPDATE reviews SET rating=?,body=?,updated_at=datetime('now') WHERE review_id=?",
                         (rating,body,existing["review_id"]))
            flash("Your review has been updated.","success")
        else:
            # Resolve store_id for this review
            if ttype == "store":
                rev_store_id = tid
            elif ttype == "product":
                _pr = conn.execute("SELECT store_id FROM products WHERE product_id=?",(tid,)).fetchone()
                rev_store_id = _pr["store_id"] if _pr else ""
            else:
                rev_store_id = ""
            conn.execute("INSERT INTO reviews(review_id,reviewer_type,reviewer_id,target_type,"
                         "target_id,order_id,store_id,rating,body) VALUES(?,?,?,?,?,?,?,?,?)",
                         (str(uuid.uuid4()),"customer",cname,ttype,tid,order_id,rev_store_id,rating,body))
            conn.execute("INSERT OR IGNORE INTO points(entity_id,entity_type,total_points) VALUES(?,?,0)",
                         (cname,"customer"))
            conn.commit(); award_points(cname,"customer","review_left")
            flash("Thank you for your review! +3 points.","success")
        conn.execute("DELETE FROM review_ai_cache WHERE target_id=? AND target_type=?",(tid,ttype))
        conn.commit(); return redirect(back)

    @app.route("/review/<review_id>/delete",methods=["POST"])
    @customer_required
    def delete_review(review_id):
        cname=session["customer_name"]; conn=get_db()
        rev=conn.execute("SELECT * FROM reviews WHERE review_id=? AND reviewer_type='customer' "
                         "AND reviewer_id=?",(review_id,cname)).fetchone()
        if not rev: abort(403)
        conn.execute("DELETE FROM reviews WHERE review_id=?",(review_id,))
        conn.execute("DELETE FROM review_ai_cache WHERE target_id=? AND target_type=?",
                     (rev["target_id"],rev["target_type"]))
        conn.commit(); flash("Review deleted.","info")
        return redirect(request.form.get("redirect_to",url_for("store_list")))

    # ── OWNER RATING OF CUSTOMER ──────────────────────────────────────────────
    @app.route("/owner/rate-customer",methods=["POST"])
    @owner_required
    def owner_rate_customer():
        order_id=sanitize(request.form.get("order_id",""),20)
        cname=sanitize(request.form.get("customer_name",""),80)
        rating,err=validate_rating(request.form.get("rating",""))
        body=sanitize(request.form.get("body",""),500)
        sid=session["store_id"]
        if err: flash(err,"error"); return redirect(url_for("owner_orders"))
        conn=get_db()
        ok=conn.execute("SELECT 1 FROM orders WHERE order_id=? AND store_id=? AND status='completed'",
                        (order_id,sid)).fetchone()
        if not ok: flash("Can only rate customers after a completed order.","error"); return redirect(url_for("owner_orders"))
        existing=conn.execute("SELECT review_id FROM reviews WHERE reviewer_type='owner' "
                              "AND reviewer_id=? AND target_type='customer' AND order_id=?",
                              (sid,order_id)).fetchone()
        if existing:
            conn.execute("UPDATE reviews SET rating=?,body=?,updated_at=datetime('now') WHERE review_id=?",
                         (rating,body,existing["review_id"]))
            flash("Customer rating updated.","success")
        else:
            conn.execute("INSERT INTO reviews(review_id,reviewer_type,reviewer_id,target_type,"
                         "target_id,order_id,store_id,rating,body) VALUES(?,?,?,?,?,?,?,?,?)",
                         (str(uuid.uuid4()),"owner",sid,"customer",cname,order_id,sid,rating,body))
            flash("Customer rated.","success")
        conn.commit(); return redirect(url_for("owner_orders"))

    # ── CART ──────────────────────────────────────────────────────────────────
    @app.route("/cart/add",methods=["POST"])
    @customer_required
    def cart_add():
        data=request.get_json(silent=True)
        if not data: return jsonify({"error":"invalid json"}),400
        cart=session.get("cart",{})
        pid=sanitize(data.get("product_id",""),50)
        qty,err=validate_quantity(data.get("qty",1))
        if err: return jsonify({"error":err}),400
        if len(cart)>=app.config["MAX_CART_ITEMS"] and pid not in cart:
            return jsonify({"error":"Cart full (max 20 items)"}),400
        row=get_db().execute("SELECT name,price,unit,store_id FROM products WHERE product_id=? AND available=1",
                             (pid,)).fetchone()
        if not row: return jsonify({"error":"Product not found"}),404
        if qty<=0: cart.pop(pid,None)
        else: cart[pid]={"name":row["name"],"price":row["price"],"unit":row["unit"],"qty":qty,"store_id":row["store_id"]}
        session["cart"]=cart
        total=sum(v["price"]*v["qty"] for v in cart.values())
        return jsonify({"cart_count":len(cart),"total":round(total,2)})

    @app.route("/cart/clear",methods=["POST"])
    def cart_clear(): session.pop("cart",None); return jsonify({"ok":True})

    @app.route("/cart/state")
    def cart_state():
        """Lightweight read-only endpoint — returns current cart count and total."""
        cart = session.get("cart", {})
        total = sum(v["price"] * v["qty"] for v in cart.values())
        return jsonify({"cart_count": len(cart), "total": round(total, 2)})

    # ── CHECKOUT ──────────────────────────────────────────────────────────────
    @app.route("/checkout/<store_id>")
    @customer_required
    def checkout(store_id):
        cart=session.get("cart",{}); sc={k:v for k,v in cart.items() if v["store_id"]==store_id}
        if not sc: flash("Your cart is empty.","error"); return redirect(url_for("store_detail",store_id=store_id))
        conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=? AND is_approved=1",(store_id,)).fetchone()
        if not store: abort(404)
        slots=conn.execute("SELECT * FROM time_slots WHERE store_id=? AND is_active=1 ORDER BY start_time",(store_id,)).fetchall()
        total=sum(v["price"]*v["qty"] for v in sc.values())
        cust_pts=get_points(session["customer_name"]); ct,ci=get_tier(cust_pts)
        dates=pickup_dates(app.config.get("PICKUP_DAYS_AHEAD",7))
        return render_template("checkout.html",store=store,cart=sc,slots=slots,
                               total=round(total,2),customer_name=session["customer_name"],
                               cust_points=cust_pts,cust_tier=ct,cust_icon=ci,pickup_dates=dates)

    @app.route("/order/place",methods=["POST"])
    @customer_required
    @rate_limit("ORDER")
    def place_order():
        store_id=request.form.get("store_id",""); slot_id=request.form.get("slot_id","")
        pickup_date=request.form.get("pickup_date",date.today().isoformat())
        cname=session["customer_name"]; cart=session.get("cart",{})
        sc={k:v for k,v in cart.items() if v["store_id"]==store_id}
        if not sc: flash("Your cart is empty.","error"); return redirect(url_for("store_list"))
        if not slot_id: flash("Please select a time slot.","error"); return redirect(url_for("checkout",store_id=store_id))
        try:
            pd=date.fromisoformat(pickup_date); today=date.today()
            max_d=today+timedelta(days=app.config.get("PICKUP_DAYS_AHEAD",7))
            if pd<today or pd>max_d: pickup_date=today.isoformat()
        except ValueError: pickup_date=date.today().isoformat()
        conn=get_db()
        if not conn.execute("SELECT 1 FROM stores WHERE store_id=? AND is_open=1 AND is_approved=1",(store_id,)).fetchone():
            flash("Store is not available.","error"); return redirect(url_for("store_list"))
        if not conn.execute("SELECT 1 FROM time_slots WHERE slot_id=? AND store_id=? AND is_active=1",(slot_id,store_id)).fetchone():
            flash("Invalid time slot.","error"); return redirect(url_for("checkout",store_id=store_id))
        oid="ORD-"+str(uuid.uuid4())[:8].upper()
        total=sum(v["price"]*v["qty"] for v in sc.values())
        conn.execute("INSERT INTO orders(order_id,customer_name,store_id,slot_id,pickup_date,total_amount) VALUES(?,?,?,?,?,?)",
                     (oid,cname,store_id,slot_id,pickup_date,round(total,2)))
        for pid,item in sc.items():
            conn.execute("INSERT INTO order_items(item_id,order_id,product_id,product_name,quantity,unit_price,subtotal) VALUES(?,?,?,?,?,?,?)",
                         (str(uuid.uuid4()),oid,pid,item["name"],item["qty"],item["price"],round(item["price"]*item["qty"],2)))
        conn.execute("INSERT OR IGNORE INTO points(entity_id,entity_type,total_points) VALUES(?,?,0)",(cname,"customer"))
        # conn.commit();# 🔔 Notify store owner about new order
        conn.commit()
        award_points(cname,"customer","order_placed",oid)
        update_analytics(store_id)
        store_row = conn.execute("SELECT name,owner_id FROM stores WHERE store_id=?",(store_id,)).fetchone()
        item_count = sum(int(v["qty"]) for v in sc.values())
        push_notification(
            "owner", store_id,
            f"New Order: {oid}",
            f"{cname} ordered {item_count} item{'s' if item_count!=1 else ''} for {pickup_date} · ₹{round(total,2)}",
            "🆕",
            f"/owner/orders?date={pickup_date}"
        )
        push_fcm_event("order_placed", "owner", store_id,
                       name=cname, amount=round(total,2), id=oid, store=store_row["name"] if store_row else "")
        award_points(cname,"customer","order_placed",oid); update_analytics(store_id)
        session["cart"]={k:v for k,v in cart.items() if v["store_id"]!=store_id}
        flash(f"Order {oid} placed for {pickup_date}! +2 points.","success")
        return redirect(url_for("order_confirmation",order_id=oid))

    @app.route("/order/<order_id>")
    @customer_required
    def order_confirmation(order_id):
        conn=get_db()
        order=conn.execute("""SELECT o.*,s.name AS store_name,ts.label AS slot_label
            FROM orders o JOIN stores s ON o.store_id=s.store_id
            JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.order_id=? AND o.customer_name=?""",(order_id,session["customer_name"])).fetchone()
        if not order: abort(404)
        items=conn.execute("SELECT * FROM order_items WHERE order_id=?",(order_id,)).fetchall()
        return render_template("order_confirmation.html",order=order,items=items,
                               cust_points=get_points(session["customer_name"]))

    @app.route("/my-orders")
    @customer_required
    def my_orders():
        name=session["customer_name"]
        orders=get_db().execute("""SELECT o.*,s.name AS store_name,ts.label AS slot_label
            FROM orders o JOIN stores s ON o.store_id=s.store_id
            JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.customer_name=? ORDER BY o.placed_at DESC""",(name,)).fetchall()
        pts=get_points(name); tier,icon=get_tier(pts)
        return render_template("my_orders.html",orders=orders,customer_name=name,
                               cust_points=pts,tier=tier,icon=icon)

    @app.route("/scan",methods=["GET","POST"])
    @customer_required
    def qr_scan():
        if request.method=="POST":
            return redirect(url_for("verify_visit",qr_data=sanitize(request.form.get("qr_data",""),200)))
        name=session["customer_name"]
        active=get_db().execute("""SELECT o.order_id,s.name AS store_name,s.qr_code,
            ts.label AS slot_label,o.status,o.pickup_date
            FROM orders o JOIN stores s ON o.store_id=s.store_id
            JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.customer_name=? AND o.status IN('placed','preparing','ready')
            ORDER BY o.pickup_date,o.placed_at DESC""",(name,)).fetchall()
        return render_template("qr_scan.html",active_orders=active,customer_name=name)

    @app.route("/verify-visit")
    @customer_required
    def verify_visit():
        qr=sanitize(request.args.get("qr_data",""),200); name=session["customer_name"]
        if not qr.startswith("STORE:"): flash("Invalid QR code.","error"); return redirect(url_for("qr_scan"))
        parts=qr.split(":"); sid=parts[1] if len(parts)>=3 else ""
        conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        if not store or store["qr_code"]!=qr: flash("QR code is not valid.","error"); return redirect(url_for("qr_scan"))
        order=conn.execute("""SELECT o.*,ts.start_time,ts.end_time,ts.label AS slot_label
            FROM orders o JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.customer_name=? AND o.store_id=? AND o.status IN('placed','preparing','ready')
            ORDER BY o.placed_at DESC LIMIT 1""",(name,sid)).fetchone()
        if not order: flash("No active order found for this store.","error"); return redirect(url_for("qr_scan"))
        if conn.execute("SELECT 1 FROM visits WHERE order_id=?",(order["order_id"],)).fetchone():
            flash("Visit already verified.","info"); return redirect(url_for("confirm_receipt",order_id=order["order_id"]))
        now=datetime.now().strftime("%H:%M"); within=order["start_time"]<=now<=order["end_time"]
        conn.execute("INSERT INTO visits(visit_id,order_id,store_id,customer_name,within_slot,verified) VALUES(?,?,?,?,?,1)",
                     (str(uuid.uuid4()),order["order_id"],sid,name,1 if within else 0))
        conn.execute("UPDATE orders SET status='visited',visit_verified=1 WHERE order_id=?",(order["order_id"],))
        push_fcm_event("order_visited","owner",sid,name=name,id=order["order_id"])
        conn.commit(); update_analytics(sid)
        if within:
            award_points(name,"customer","visit_verified_ontime",order["order_id"])
            award_points(sid,"store","visit_verified_ontime",order["order_id"])
            flash("Visit verified! On time. +5 points!","success")
        else:
            award_points(name,"customer","late_arrival",order["order_id"])
            flash("Visit verified, but after slot window. −5 points.","warning")
        return redirect(url_for("confirm_receipt",order_id=order["order_id"]))

    @app.route("/confirm-receipt/<order_id>",methods=["GET","POST"])
    @customer_required
    def confirm_receipt(order_id):
        conn=get_db()
        order=conn.execute("""SELECT o.*,s.name AS store_name,ts.label AS slot_label
            FROM orders o JOIN stores s ON o.store_id=s.store_id
            JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.order_id=? AND o.customer_name=?""",(order_id,session["customer_name"])).fetchone()
        if not order: abort(404)
        items=conn.execute("SELECT * FROM order_items WHERE order_id=?",(order_id,)).fetchall()
        if request.method=="POST":
            conn.execute("UPDATE orders SET customer_confirmed=1 WHERE order_id=?",(order_id,))
            o=conn.execute("SELECT store_confirmed,store_id,customer_name FROM orders WHERE order_id=?",(order_id,)).fetchone()
            if o["store_confirmed"]:
                conn.execute("UPDATE orders SET status='completed',completed_at=datetime('now') WHERE order_id=?",(order_id,))
                conn.commit()
                award_points(o["customer_name"],"customer","order_complete",order_id)
                award_points(o["store_id"],"store","order_complete",order_id)
                _sname = conn.execute("SELECT name FROM stores WHERE store_id=?",(o["store_id"],)).fetchone()
                sname = _sname["name"] if _sname else ""
                push_fcm_event("order_complete","store",o["store_id"],store=sname,id=order_id,name=session["customer_name"])
                update_analytics(o["store_id"]); flash("Order completed! +10 points!","success")
            else:
                conn.commit()
                push_fcm_event("order_handover_cust","owner",o["store_id"],
                               name=session["customer_name"], id=order_id)
                push_notification("owner",o["store_id"],
                                  "🤝 Customer Confirmed",f"{session['customer_name']} confirmed receipt. Please confirm your side.","🤝",f"/owner/orders")
                flash("You confirmed receipt. Waiting for store to confirm.","info")
            return redirect(url_for("my_orders"))
        return render_template("confirm_receipt.html",order=order,items=items,customer_name=session["customer_name"])

    @app.route("/my-points")
    @customer_required
    def my_points():
        name=session["customer_name"]; pts=get_points(name); tier,icon=get_tier(pts)
        txns=get_db().execute("SELECT * FROM transactions WHERE entity_id=? ORDER BY created_at DESC LIMIT 20",(name,)).fetchall()
        store_ratings=get_db().execute(
            "SELECT r.*,s.name AS store_name FROM reviews r JOIN stores s ON r.reviewer_id=s.store_id "
            "WHERE r.target_type='customer' AND r.target_id=? ORDER BY r.created_at DESC",(name,)).fetchall()
        avg_r,r_cnt=get_avg_rating("customer",name)
        return render_template("my_points.html",customer_name=name,points=pts,tier=tier,icon=icon,
                               transactions=txns,store_ratings=store_ratings,
                               cust_avg_rating=avg_r,cust_rating_count=r_cnt)

    # ── OWNER AUTH ────────────────────────────────────────────────────────────
    @app.route("/owner/register",methods=["GET","POST"])
    @rate_limit("REGISTER")
    def owner_register():
        if session.get("role")=="customer":
            flash("You are signed in as a customer. Log out first.","error"); return redirect(url_for("store_list"))
        if session.get("owner_id"): return redirect(url_for("owner_dashboard"))
        if request.method=="POST":
            username=sanitize(request.form.get("username",""),40).lower()
            password=request.form.get("password",""); confirm=request.form.get("confirm","")
            full_name=sanitize(request.form.get("full_name",""),app.config["MAX_NAME_LEN"])
            phone=sanitize(request.form.get("phone",""),20)
            store_name=sanitize(request.form.get("store_name",""),app.config["MAX_STORE_NAME_LEN"])
            address=sanitize(request.form.get("address",""),app.config["MAX_ADDRESS_LEN"])
            category=request.form.get("category","General")
            if category not in STORE_CATEGORIES: category="General"
            errors=[]
            if not all([username,full_name,store_name]): errors.append("Username, full name and store name required.")
            err=validate_username(username)
            if err: errors.append(err)
            if len(password)<6: errors.append("Password must be at least 6 characters.")
            if len(password)>app.config["MAX_PASSWORD_LEN"]: errors.append("Password too long.")
            if password!=confirm: errors.append("Passwords do not match.")
            if errors:
                for e in errors: flash(e,"error")
                return render_template("owner_register.html",categories=STORE_CATEGORIES,form=request.form)
            conn=get_db()
            if conn.execute("SELECT 1 FROM owners WHERE username=?",(username,)).fetchone():
                flash("Username already taken.","error")
                return render_template("owner_register.html",categories=STORE_CATEGORIES,form=request.form)
            oid="own_"+str(uuid.uuid4())[:8]
            conn.execute("INSERT INTO owners(owner_id,username,password,full_name,phone) VALUES(?,?,?,?,?)",
                         (oid,username,generate_password_hash(password),full_name,phone))
            sid="store_"+str(uuid.uuid4())[:8]; qr=_make_qr_payload(sid)
            lat, lng = geocode_location(address) if address else (None, None)
            conn.execute("INSERT INTO stores(store_id,owner_id,name,owner_name,address,category,qr_code,latitude,longitude) VALUES(?,?,?,?,?,?,?,?,?)",
                         (sid,oid,store_name,full_name,address,category,qr,lat,lng))
            conn.execute("INSERT INTO store_analytics(store_id) VALUES(?)",(sid,))
            conn.execute("INSERT INTO points(entity_id,entity_type,total_points) VALUES(?,?,0)",(sid,"store"))
            conn.commit(); _insert_default_slots(conn,sid); conn.commit()
            session["owner_id"]=oid; session["owner_name"]=full_name
            session["store_id"]=sid; session["role"]="owner"; session.permanent=True
            flash(f"Welcome, {full_name}! Your store '{store_name}' is live.","success")
            return redirect(url_for("owner_dashboard"))
        return render_template("owner_register.html",categories=STORE_CATEGORIES,form={})

    @app.route("/owner/login",methods=["GET","POST"])
    @rate_limit("LOGIN")
    def owner_login():
        if session.get("role")=="customer":
            flash("You are signed in as a customer. Log out first.","error"); return redirect(url_for("store_list"))
        if session.get("owner_id"): return redirect(url_for("owner_dashboard"))
        if request.method=="POST":
            username=sanitize(request.form.get("username",""),40).lower()
            password=request.form.get("password",""); conn=get_db()
            owner=conn.execute("SELECT * FROM owners WHERE username=? AND is_active=1",(username,)).fetchone()
            if not owner or not check_password_hash(owner["password"],password):
                app.logger.warning("Failed owner login: %s ip=%s",username,request.remote_addr)
                flash("Invalid username or password.","error"); return render_template("owner_login.html")
            store=conn.execute("SELECT * FROM stores WHERE owner_id=?",(owner["owner_id"],)).fetchone()
            session["owner_id"]=owner["owner_id"]; session["owner_name"]=owner["full_name"]
            session["store_id"]=store["store_id"] if store else None
            session["role"]="owner"; session.permanent=True
            flash(f"Welcome back, {owner['full_name']}!","success"); return redirect(url_for("owner_dashboard"))
        return render_template("owner_login.html")

    @app.route("/owner/logout")
    def owner_logout(): session.clear(); return redirect(url_for("index"))

    @app.route("/owner")
    def owner_home():
        return redirect(url_for("owner_dashboard") if session.get("owner_id") else url_for("owner_login"))

    # ── OWNER DASHBOARD ───────────────────────────────────────────────────────
    @app.route("/owner/dashboard")
    @owner_required
    def owner_dashboard():
        sid=session["store_id"]; conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        analytics=conn.execute("SELECT * FROM store_analytics WHERE store_id=?",(sid,)).fetchone()
        pending=conn.execute("""SELECT o.*,ts.label AS slot_label
            FROM orders o JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.store_id=? AND o.status IN('placed','preparing')
            ORDER BY o.pickup_date, ts.start_time""", (sid,)).fetchall()
        today_orders=conn.execute("""SELECT o.*,ts.label AS slot_label
            FROM orders o JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.store_id=? AND o.pickup_date=date('now')
            ORDER BY ts.start_time""", (sid,)).fetchall()
        ready=[o for o in today_orders if o["status"]=="ready"]
        visited=[o for o in today_orders if o["status"]=="visited"]
        completed=[o for o in today_orders if o["status"]=="completed"]
        pts=get_points(sid); tier,icon=get_tier(pts)
        peak_slots=json.loads(analytics["peak_slots"]) if analytics and analytics["peak_slots"] else {}
        avg_r,r_cnt=get_avg_rating("store",sid)
        return render_template("owner_dashboard.html",store=store,analytics=analytics,
                               pending=pending,ready=ready,visited=visited,completed=completed,
                               store_points=pts,tier=tier,icon=icon,peak_slots=peak_slots,
                               avg_rating=avg_r,rating_count=r_cnt)

    @app.route("/owner/store/settings",methods=["GET","POST"])
    @owner_required
    def owner_store_settings():
        sid=session["store_id"]; conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        if request.method=="POST":
            name=sanitize(request.form.get("name",""),app.config["MAX_STORE_NAME_LEN"])
            address=sanitize(request.form.get("address",""),app.config["MAX_ADDRESS_LEN"])
            description=sanitize(request.form.get("description",""),500)
            category=request.form.get("category","General"); is_open=1 if request.form.get("is_open") else 0
            if category not in STORE_CATEGORIES: category="General"
            if not name:
                flash("Store name cannot be empty.","error")
            else:
                img_b64 = store["image_b64"]
                img_mime = store["image_mime"] or "image/jpeg"
                if "image" in request.files and request.files["image"].filename:
                    try:
                        img_b64, img_mime = process_image(request.files["image"])
                    except ValueError as e:
                        flash(str(e),"error")
                        return render_template("owner_store_settings.html",store=store,categories=STORE_CATEGORIES)
                lat, lng = store["latitude"], store["longitude"]
                if address and (address != store["address"] or lat is None or lng is None):
                    new_lat, new_lng = geocode_location(address)
                    if new_lat is not None:
                        lat, lng = new_lat, new_lng
                conn.execute(
                    "UPDATE stores SET name=?,address=?,category=?,is_open=?,description=?,image_b64=?,image_mime=?,latitude=?,longitude=? WHERE store_id=?",
                    (name,address,category,is_open,description,img_b64,img_mime,lat,lng,sid))
                conn.commit()
                flash("Store settings updated.","success"); return redirect(url_for("owner_store_settings"))
        return render_template("owner_store_settings.html",store=store,categories=STORE_CATEGORIES)

    # ── OWNER PRODUCTS ────────────────────────────────────────────────────────
    @app.route("/owner/products")
    @owner_required
    def owner_products():
        sid=session["store_id"]; conn=get_db()
        products=conn.execute("SELECT * FROM products WHERE store_id=? ORDER BY category,name",(sid,)).fetchall()
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        return render_template("owner_products.html",products=products,store=store)

    def _product_form_post(product_id=None):
        """Shared logic for add and edit."""
        sid=session["store_id"]; conn=get_db()
        name=sanitize(request.form.get("name",""),app.config["MAX_PRODUCT_NAME_LEN"])
        price,err=validate_price(request.form.get("price"))
        errors=[]
        if not name: errors.append("Product name required.")
        if err: errors.append(err)
        if errors:
            for e in errors: flash(e,"error")
            return None, None, None
        img_b64=img_mime=None
        if product_id:
            row=conn.execute("SELECT image_b64,image_mime FROM products WHERE product_id=? AND store_id=?",
                             (product_id,sid)).fetchone()
            if row: img_b64,img_mime=row["image_b64"],row["image_mime"]
        if "image" in request.files and request.files["image"].filename:
            try: img_b64,img_mime=process_image(request.files["image"])
            except ValueError as e: flash(str(e),"error"); return None,None,None
        if not img_mime: img_mime="image/jpeg"
        fields=(
            name,price,
            sanitize(request.form.get("unit","piece"),20),
            sanitize(request.form.get("category","General"),50),
            sanitize(request.form.get("description",""),2000),
            img_b64,img_mime,
            sanitize(request.form.get("brand",""),80),
            sanitize(request.form.get("model",""),80),
            sanitize(request.form.get("origin",""),80),
            sanitize(request.form.get("dimensions",""),100),
            sanitize(request.form.get("weight",""),50),
            sanitize(request.form.get("material",""),100),
            sanitize(request.form.get("spec1_key",""),60),
            sanitize(request.form.get("spec1_val",""),100),
            sanitize(request.form.get("spec2_key",""),60),
            sanitize(request.form.get("spec2_val",""),100),
            sanitize(request.form.get("spec3_key",""),60),
            sanitize(request.form.get("spec3_val",""),100),
        )
        return conn, sid, fields

    @app.route("/owner/update_stock", methods=["POST"])
    @owner_required
    def update_stock():
        pid = request.form.get("product_id")
        stock = int(request.form.get("stock"))

        conn = get_db()
        conn.execute(
            "UPDATE products SET stock=? WHERE product_id=?",
            (stock, pid)
        )
        conn.commit()

        return redirect(url_for("owner_products"))

    @app.route("/owner/product/add",methods=["GET","POST"])
    @owner_required
    def owner_product_add():
        sid=session["store_id"]
        store=get_db().execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        if request.method=="POST":
            conn,_,fields=_product_form_post()
            if fields is None: return render_template("owner_product_form.html",store=store,product=None,
                                                       categories=STORE_CATEGORIES,units=PRODUCT_UNITS)
            pid=str(uuid.uuid4())
            conn.execute("""INSERT INTO products
                (product_id,store_id,name,price,unit,category,description,image_b64,image_mime,
                 brand,model,origin,dimensions,weight,material,
                 spec1_key,spec1_val,spec2_key,spec2_val,spec3_key,spec3_val)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (pid,sid)+fields); conn.commit()
            flash("Product added.","success"); return redirect(url_for("owner_products"))
        return render_template("owner_product_form.html",store=store,product=None,
                               categories=STORE_CATEGORIES,units=PRODUCT_UNITS)

    @app.route("/owner/product/<product_id>/edit",methods=["GET","POST"])
    @owner_required
    def owner_product_edit(product_id):
        sid=session["store_id"]; conn=get_db()
        product=conn.execute("SELECT * FROM products WHERE product_id=? AND store_id=?",(product_id,sid)).fetchone()
        if not product: abort(404)
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        if request.method=="POST":
            c,_,fields=_product_form_post(product_id)
            if fields is None: return render_template("owner_product_form.html",store=store,product=product,
                                                       categories=STORE_CATEGORIES,units=PRODUCT_UNITS)
            c.execute("""UPDATE products SET name=?,price=?,unit=?,category=?,description=?,
                image_b64=?,image_mime=?,brand=?,model=?,origin=?,dimensions=?,weight=?,material=?,
                spec1_key=?,spec1_val=?,spec2_key=?,spec2_val=?,spec3_key=?,spec3_val=?
                WHERE product_id=? AND store_id=?""", fields+(product_id,sid)); c.commit()
            flash("Product updated.","success"); return redirect(url_for("owner_products"))
        return render_template("owner_product_form.html",store=store,product=product,
                               categories=STORE_CATEGORIES,units=PRODUCT_UNITS)

    @app.route("/owner/product/<product_id>/toggle",methods=["POST"])
    @owner_required
    def owner_product_toggle(product_id):
        sid=session["store_id"]; conn=get_db()
        conn.execute("UPDATE products SET available=1-available WHERE product_id=? AND store_id=?",(product_id,sid))
        conn.commit(); flash("Availability updated.","info"); return redirect(url_for("owner_products"))

    @app.route("/owner/product/<product_id>/delete",methods=["POST"])
    @owner_required
    def owner_product_delete(product_id):
        sid=session["store_id"]; conn=get_db()
        conn.execute("DELETE FROM products WHERE product_id=? AND store_id=?",(product_id,sid))
        conn.commit(); flash("Product deleted.","info"); return redirect(url_for("owner_products"))

    # ── OWNER ORDERS ──────────────────────────────────────────────────────────
    @app.route("/owner/orders")
    @owner_required
    def owner_orders():
        sid=session["store_id"]; conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        # Optional date filter; if none, show ALL orders grouped by date then slot
        filter_date=request.args.get("date","")
        if filter_date:
            orders=conn.execute("""SELECT o.*,ts.label AS slot_label,ts.start_time,ts.end_time
                FROM orders o JOIN time_slots ts ON o.slot_id=ts.slot_id
                WHERE o.store_id=? AND o.pickup_date=?
                ORDER BY ts.start_time,o.placed_at DESC""", (sid,filter_date)).fetchall()
        else:
            orders=conn.execute("""SELECT o.*,ts.label AS slot_label,ts.start_time,ts.end_time
                FROM orders o JOIN time_slots ts ON o.slot_id=ts.slot_id
                WHERE o.store_id=?
                ORDER BY o.pickup_date DESC, ts.start_time, o.placed_at DESC""", (sid,)).fetchall()
        # Group: when showing all, key = "DATE · SLOT"; when filtered, key = slot only
        slots_map={}
        for o in orders:
            key = (f"{o['pickup_date']} · " if not filter_date else "") + o["slot_label"]
            slots_map.setdefault(key,[]).append(o)
        order_dates=[r[0] for r in conn.execute(
            "SELECT DISTINCT pickup_date FROM orders WHERE store_id=? ORDER BY pickup_date DESC LIMIT 30",(sid,)).fetchall()]
        cust_ratings={r["target_id"]:r["rating"] for r in conn.execute(
            "SELECT target_id,rating FROM reviews WHERE reviewer_type='owner' AND reviewer_id=?",(sid,)).fetchall()}
        return render_template("owner_orders.html",store=store,slots_map=slots_map,
                               filter_date=filter_date,order_dates=order_dates,customer_ratings=cust_ratings)

    @app.route("/owner/order/<order_id>")
    @owner_required
    def owner_order_detail(order_id):
        sid=session["store_id"]; conn=get_db()
        order=conn.execute("""SELECT o.*,s.name AS store_name,ts.label AS slot_label
            FROM orders o JOIN stores s ON o.store_id=s.store_id
            JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.order_id=? AND o.store_id=?""",(order_id,sid)).fetchone()
        if not order: abort(404)
        items=conn.execute("SELECT * FROM order_items WHERE order_id=?",(order_id,)).fetchall()
        existing_rating=conn.execute("SELECT * FROM reviews WHERE reviewer_type='owner' AND reviewer_id=? "
                                     "AND target_type='customer' AND order_id=?",(sid,order_id)).fetchone()
        return render_template("owner_order_detail.html",order=order,items=items,existing_rating=existing_rating)

    @app.route("/owner/order/<order_id>/ready",methods=["POST"])
    @owner_required
    def mark_order_ready(order_id):
        sid=session["store_id"]; conn=get_db()
        rows=conn.execute("UPDATE orders SET status='ready' WHERE order_id=? AND store_id=? "
                          "AND status IN('placed','preparing')",(order_id,sid)).rowcount
        conn.commit()
        if rows:
            o_row = conn.execute(
                "SELECT customer_name, s.name AS store_name FROM orders "
                "JOIN stores s ON orders.store_id=s.store_id WHERE order_id=?", (order_id,)).fetchone()
            if o_row:
                push_fcm_event("order_ready", "customer", o_row["customer_name"],
                               store=o_row["store_name"], id=order_id)
                push_notification("customer", o_row["customer_name"],
                                  "✅ Order Ready!", f"Your order at {o_row['store_name']} is ready for pickup!", "✅", "/my-orders")
        flash("Order marked as ready!" if rows else "Could not update.","success" if rows else "error")
        return redirect(url_for("owner_orders"))

    @app.route("/owner/order/<order_id>/confirm",methods=["POST"])
    @owner_required
    def owner_confirm_handover(order_id):
        sid=session["store_id"]; conn=get_db()
        conn.execute("UPDATE orders SET store_confirmed=1 WHERE order_id=? AND store_id=?",(order_id,sid))
        o=conn.execute("SELECT customer_confirmed,store_id,customer_name FROM orders WHERE order_id=?",(order_id,)).fetchone()
        if not o: conn.commit(); flash("Order not found.","error"); return redirect(url_for("owner_orders"))
        if o["customer_confirmed"]:
            conn.execute("UPDATE orders SET status='completed',completed_at=datetime('now') WHERE order_id=?",(order_id,))
            conn.commit()
            award_points(o["customer_name"],"customer","order_complete",order_id)
            award_points(o["store_id"],"store","order_complete",order_id)
            _sname = conn.execute("SELECT name FROM stores WHERE store_id=?",(o["store_id"],)).fetchone()
            sname = _sname["name"] if _sname else ""
            push_fcm_event("order_complete","customer",o["customer_name"],store=sname,id=order_id)
            update_analytics(o["store_id"]); flash("Order completed! +10 store points!","success")
        else:
            conn.commit()
            push_fcm_event("order_handover_owner","customer",o["customer_name"],
                           store="", id=order_id, name=session.get("owner_name","Store"))
            push_notification("customer",o["customer_name"],
                              "🤝 Handover Confirmed","Store confirmed your order handover. Please confirm receipt.","🤝",f"/confirm-receipt/{order_id}")
            flash("Handover confirmed. Waiting for customer to confirm.","info")
        return redirect(url_for("owner_orders"))

    @app.route("/owner/qr")
    @owner_required
    def owner_qr():
        sid=session["store_id"]
        store=get_db().execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        return render_template("owner_qr.html",store=store,qr_img=generate_qr_b64(store["qr_code"]),qr_fallback=store["qr_code"])

    @app.route("/owner/analytics")
    @owner_required
    def owner_analytics():
        sid=session["store_id"]; conn=get_db()
        store=conn.execute("SELECT * FROM stores WHERE store_id=?",(sid,)).fetchone()
        analytics=conn.execute("SELECT * FROM store_analytics WHERE store_id=?",(sid,)).fetchone()
        recent=conn.execute("""SELECT o.*,ts.label AS slot_label FROM orders o
            JOIN time_slots ts ON o.slot_id=ts.slot_id
            WHERE o.store_id=? ORDER BY o.placed_at DESC LIMIT 15""",(sid,)).fetchall()
        txns=conn.execute("SELECT * FROM transactions WHERE entity_id=? ORDER BY created_at DESC LIMIT 10",(sid,)).fetchall()
        pts=get_points(sid); tier,icon=get_tier(pts)
        peak_slots=json.loads(analytics["peak_slots"]) if analytics and analytics["peak_slots"] else {}
        avg_r,r_cnt=get_avg_rating("store",sid)
        reviews=conn.execute("SELECT * FROM reviews WHERE target_type='store' AND target_id=? "
                             "ORDER BY created_at DESC LIMIT 10",(sid,)).fetchall()
        ai_summary=get_ai_review_summary("store",sid)
        return render_template("owner_analytics.html",store=store,analytics=analytics,
                               store_points=pts,tier=tier,icon=icon,peak_slots=peak_slots,
                               recent_orders=recent,transactions=txns,
                               avg_rating=avg_r,rating_count=r_cnt,reviews=reviews,ai_summary=ai_summary)

    # ── ADMIN ─────────────────────────────────────────────────────────────────
    ADMIN_PREFIX="/control-panel"
    @app.route(f"{ADMIN_PREFIX}/login",methods=["GET","POST"])
    @rate_limit("LOGIN")
    def admin_login():
        if session.get("admin_id"): return redirect(url_for("admin_dashboard"))
        if request.method=="POST":
            u=sanitize(request.form.get("username",""),40).lower(); p=request.form.get("password","")
            conn=get_db(); admin=conn.execute("SELECT * FROM admins WHERE username=?",(u,)).fetchone()
            if not admin or not check_password_hash(admin["password"],p):
                flash("Invalid credentials.","error"); return render_template("admin/login.html")
            session["admin_id"]=admin["admin_id"]; session["admin_name"]=admin["username"]
            session["role"]="admin"; session.permanent=True; return redirect(url_for("admin_dashboard"))
        return render_template("admin/login.html")
    @app.route(f"{ADMIN_PREFIX}/logout")
    def admin_logout(): session.clear(); return redirect(url_for("index"))
    @app.route(f"{ADMIN_PREFIX}/")
    @admin_required
    def admin_dashboard():
        conn=get_db()
        stats={k:conn.execute(q).fetchone()[0] for k,q in [
            ("owners","SELECT COUNT(*) FROM owners"),("stores","SELECT COUNT(*) FROM stores"),
            ("orders","SELECT COUNT(*) FROM orders"),
            ("completed","SELECT COUNT(*) FROM orders WHERE status='completed'"),
            ("revenue","SELECT COALESCE(SUM(total_amount),0) FROM orders WHERE status='completed'"),
            ("reviews","SELECT COUNT(*) FROM reviews")]}
        recent=conn.execute("""SELECT o.*,s.name AS store_name FROM orders o
            JOIN stores s ON o.store_id=s.store_id ORDER BY o.placed_at DESC LIMIT 10""").fetchall()
        return render_template("admin/dashboard.html",stats=stats,recent_orders=recent)
    @app.route(f"{ADMIN_PREFIX}/owners")
    @admin_required
    def admin_owners():
        owners=get_db().execute("""SELECT o.*,s.store_id,s.name AS store_name,s.is_open,s.is_approved,s.category
            FROM owners o LEFT JOIN stores s ON s.owner_id=o.owner_id ORDER BY o.created_at DESC""").fetchall()
        return render_template("admin/owners.html",owners=owners)
    @app.route(f"{ADMIN_PREFIX}/owners/<owner_id>/toggle",methods=["POST"])
    @admin_required
    def admin_toggle_owner(owner_id):
        conn=get_db(); conn.execute("UPDATE owners SET is_active=1-is_active WHERE owner_id=?",(owner_id,)); conn.commit()
        flash("Owner status updated.","success"); return redirect(url_for("admin_owners"))
    @app.route(f"{ADMIN_PREFIX}/stores")
    @admin_required
    def admin_stores():
        stores=get_db().execute("""SELECT s.*,o.username,o.full_name AS owner_full_name,
            (SELECT COUNT(*) FROM orders WHERE store_id=s.store_id) AS order_count
            FROM stores s JOIN owners o ON s.owner_id=o.owner_id ORDER BY s.created_at DESC""").fetchall()
        return render_template("admin/stores.html",stores=stores)
    @app.route(f"{ADMIN_PREFIX}/stores/<store_id>/toggle-approved",methods=["POST"])
    @admin_required
    def admin_toggle_store_approved(store_id):
        conn=get_db(); conn.execute("UPDATE stores SET is_approved=1-is_approved WHERE store_id=?",(store_id,)); conn.commit()
        flash("Store approval updated.","success"); return redirect(url_for("admin_stores"))
    @app.route(f"{ADMIN_PREFIX}/reviews")
    @admin_required
    def admin_reviews():
        reviews=get_db().execute("""SELECT r.*,
            CASE r.target_type WHEN 'product' THEN(SELECT name FROM products WHERE product_id=r.target_id)
            WHEN 'store' THEN(SELECT name FROM stores WHERE store_id=r.target_id) ELSE r.target_id END AS target_name
            FROM reviews r ORDER BY r.created_at DESC LIMIT 100""").fetchall()
        return render_template("admin/reviews.html",reviews=reviews)
    @app.route(f"{ADMIN_PREFIX}/reviews/<review_id>/delete",methods=["POST"])
    @admin_required
    def admin_delete_review(review_id):
        conn=get_db(); rev=conn.execute("SELECT * FROM reviews WHERE review_id=?",(review_id,)).fetchone()
        if rev:
            conn.execute("DELETE FROM reviews WHERE review_id=?",(review_id,))
            conn.execute("DELETE FROM review_ai_cache WHERE target_id=? AND target_type=?",(rev["target_id"],rev["target_type"]))
            conn.commit()
        flash("Review deleted.","info"); return redirect(url_for("admin_reviews"))
    @app.route(f"{ADMIN_PREFIX}/orders")
    @admin_required
    def admin_orders():
        status=request.args.get("status","")
        orders=get_db().execute(
            f"SELECT o.*,s.name AS store_name FROM orders o JOIN stores s ON o.store_id=s.store_id "
            f"WHERE 1=1{' AND o.status=?' if status else''} ORDER BY o.placed_at DESC LIMIT 100",
            (status,) if status else()).fetchall()
        return render_template("admin/orders.html",orders=orders,status_filter=status)
    @app.route(f"{ADMIN_PREFIX}/customers")
    @admin_required
    def admin_customers():
        custs=get_db().execute("""SELECT p.entity_id AS customer_name,p.total_points,
            COUNT(o.order_id) AS order_count,MAX(o.placed_at) AS last_order,
            (SELECT AVG(rating) FROM reviews WHERE target_type='customer' AND target_id=p.entity_id) AS avg_rating
            FROM points p LEFT JOIN orders o ON o.customer_name=p.entity_id
            WHERE p.entity_type='customer' GROUP BY p.entity_id ORDER BY p.total_points DESC""").fetchall()
        return render_template("admin/customers.html",customers=custs)
    @app.route(f"{ADMIN_PREFIX}/settings",methods=["GET","POST"])
    @admin_required
    def admin_settings():
        conn=get_db()
        if request.method=="POST" and request.form.get("action")=="change_password":
            cur=request.form.get("current_password",""); npw=request.form.get("new_password","")
            cfm=request.form.get("confirm_password","")
            adm=conn.execute("SELECT * FROM admins WHERE admin_id=?",(session["admin_id"],)).fetchone()
            if not check_password_hash(adm["password"],cur): flash("Current password incorrect.","error")
            elif len(npw)<8: flash("New password must be at least 8 characters.","error")
            elif npw!=cfm: flash("New passwords do not match.","error")
            else:
                conn.execute("UPDATE admins SET password=? WHERE admin_id=?",(generate_password_hash(npw),session["admin_id"]))
                conn.commit(); flash("Password updated.","success")
        stats={k:conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
               for k,t in [("owners","owners"),("stores","stores"),("orders","orders"),("reviews","reviews")]}
        return render_template("admin/settings.html",stats=stats)

    # ── FCM TOKEN REGISTRATION ────────────────────────────────────────────────
    @app.route("/api/fcm-token", methods=["POST"])
    def register_fcm_token():
        """Register or refresh an FCM device token for the current user."""
        data = request.get_json(silent=True) or {}
        token = (data.get("token") or "").strip()
        if not token:
            return jsonify({"ok": False, "error": "token required"}), 400

        role = session.get("role")
        if role == "customer":
            rtype = "customer"
            rid   = session.get("customer_name", "")
        elif role == "owner":
            rtype = "owner"
            rid   = session.get("store_id", "")
        else:
            return jsonify({"ok": False, "error": "not logged in"}), 401

        if not rid:
            return jsonify({"ok": False, "error": "session incomplete"}), 401

        conn = get_db()
        conn.execute("""
            INSERT INTO fcm_tokens(token_id, recipient_type, recipient_id, fcm_token, last_seen)
            VALUES(?, ?, ?, ?, datetime('now'))
            ON CONFLICT(fcm_token) DO UPDATE SET
                recipient_type=excluded.recipient_type,
                recipient_id=excluded.recipient_id,
                last_seen=datetime('now')
        """, (str(uuid.uuid4()), rtype, rid, token))
        conn.commit()
        return jsonify({"ok": True})

    @app.route("/firebase-messaging-sw.js")
    def firebase_sw():
        """Serve the Firebase service worker from static/ at the root path (required by FCM)."""
        from flask import current_app
        return current_app.send_static_file("firebase-messaging-sw.js"), 200, {
            "Content-Type": "application/javascript; charset=utf-8",
            "Service-Worker-Allowed": "/",
        }

    # ── NOTIFICATIONS ─────────────────────────────────────────────────────────

    def _get_notifs(recipient_type, recipient_id, limit=50, unread_only=False):
        q = "SELECT * FROM notifications WHERE recipient_type=? AND recipient_id=?"
        if unread_only: q += " AND is_read=0"
        q += " ORDER BY created_at DESC LIMIT ?"
        return get_db().execute(q, (recipient_type, recipient_id, limit)).fetchall()

    @app.route("/notifications")
    @customer_required
    def customer_notifications():
        cname = session["customer_name"]
        notifs = _get_notifs("customer", cname)
        # Mark all as read
        get_db().execute(
            "UPDATE notifications SET is_read=1 WHERE recipient_type='customer' AND recipient_id=?",
            (cname,))
        get_db().commit()
        return render_template("notifications.html", notifs=notifs,
                               customer_name=cname, page_title="My Notifications")

    @app.route("/owner/notifications")
    @owner_required
    def owner_notifications():
        sid = session["store_id"]
        notifs = _get_notifs("owner", sid)
        get_db().execute(
            "UPDATE notifications SET is_read=1 WHERE recipient_type='owner' AND recipient_id=?",
            (sid,))
        get_db().commit()
        return render_template("notifications.html", notifs=notifs,
                               customer_name=session.get("owner_name"), page_title="Store Notifications")

    @app.route("/notifications/mark-read", methods=["POST"])
    def mark_notifs_read():
        role = session.get("role")
        if role == "customer":
            cname = session.get("customer_name")
            if cname:
                get_db().execute(
                    "UPDATE notifications SET is_read=1 WHERE recipient_type='customer' AND recipient_id=?",
                    (cname,))
                get_db().commit()
        elif role == "owner":
            sid = session.get("store_id")
            if sid:
                get_db().execute(
                    "UPDATE notifications SET is_read=1 WHERE recipient_type='owner' AND recipient_id=?",
                    (sid,))
                get_db().commit()
        return jsonify({"ok": True})

    @app.route("/admin/send-notification", methods=["POST"])
    @admin_required
    def admin_send_notification():
        """Admin broadcasts a custom notification to a customer or owner/store."""
        rtype = request.form.get("recipient_type", "customer")
        rid   = sanitize(request.form.get("recipient_id", ""), 80)
        title = sanitize(request.form.get("title", ""), 100)
        body  = sanitize(request.form.get("body", ""), 500)
        icon  = sanitize(request.form.get("icon", "📢"), 8)
        if not all([rtype in ("customer", "owner"), rid, title]):
            flash("Recipient type, ID and title are required.", "error")
        else:
            push_notification(rtype, rid, title, body, icon)
            flash(f"Notification sent to {rtype} {rid}.", "success")
        return redirect(url_for("admin_notifications"))

    @app.route("/control-panel/notifications")
    @admin_required
    def admin_notifications():
        conn = get_db()
        recent = conn.execute(
            "SELECT * FROM notifications ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
        # Counts
        unread_c = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE is_read=0 AND recipient_type='customer'").fetchone()[0]
        unread_o = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE is_read=0 AND recipient_type='owner'").fetchone()[0]
        return render_template("admin/notifications.html",
                               notifications=recent, unread_c=unread_c, unread_o=unread_o)

    # ── CONTEXT PROCESSOR ─────────────────────────────────────────────────────
    @app.context_processor
    def inject_globals():
        cart=session.get("cart",{}); cc=len(cart); ct=sum(v["price"]*v["qty"] for v in cart.values())
        # Unread notification counts for nav badge
        notif_count = 0
        try:
            role = session.get("role")
            if role == "customer" and session.get("customer_name"):
                notif_count = get_unread_count("customer", session["customer_name"])
            elif role == "owner" and session.get("store_id"):
                notif_count = get_unread_count("owner", session["store_id"])
        except Exception:
            pass
        return dict(cart_count=cc,cart_total=round(ct,2),get_tier=get_tier,get_points=get_points,
                    owner_name=session.get("owner_name"),user_role=session.get("role"),
                    stars_html=stars_html, notif_count=notif_count)

# ── ENTRY POINT ───────────────────────────────────────────────────────────────
app = create_app(os.environ.get("FLASK_ENV","development"))
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))