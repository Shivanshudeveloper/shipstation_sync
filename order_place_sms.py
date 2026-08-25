import os
import re
import sys
import time
import json
import sqlite3
import atexit
import logging
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo  # stdlib (Python 3.9+); no pip needed
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from sendblue_api import SendblueAPI

load_dotenv()

# ---- Config ----
WC_URL = os.environ["WC_URL"].rstrip("/")
WC_AUTH = HTTPBasicAuth(os.environ["WC_KEY"], os.environ["WC_SECRET"])

SENDBLUE_API_KEY = os.environ["SENDBLUE_API_KEY"]
SENDBLUE_API_SECRET = os.environ["SENDBLUE_API_SECRET"]
FROM_NUMBER = "+17274156251"
SEND_URL = "https://api.sendblue.co/api/send-message"

# ---- ShipStation config ----
# Order creation uses the V1 API (key + secret, Basic Auth).
# (Inventory decrement was removed — the ShipStation team handles stock/fulfillment
#  manually from their dashboard. We only push the order here.)
SHIP_V1_KEY = os.environ.get("SHIP_API_KEY")
SHIP_V1_SECRET = os.environ.get("SHIP_SECRET_KEY")
SHIP_V1_BASE = "https://ssapi.shipstation.com"
_SHIP_ENABLED = bool(SHIP_V1_KEY and SHIP_V1_SECRET)
SHIP_V1_AUTH = HTTPBasicAuth(SHIP_V1_KEY, SHIP_V1_SECRET) if (SHIP_V1_KEY and SHIP_V1_SECRET) else None

# SDK client used ONLY to create/update the contact (with a proper name) before
# sending. The send-message endpoint doesn't set names reliably, so we do this first.
sb = SendblueAPI(
    api_key=SENDBLUE_API_KEY,
    api_secret=SENDBLUE_API_SECRET,
)

# Only these statuses count as a "real" new order worth confirming.
# We deliberately DO NOT include pending/failed (those are the abandoned-cart script's job).
TARGET_STATUSES = ["processing", "completed", "on-hold"]

# How far back to look each run. Overlap is harmless because dedup is by order ID.
LOOKBACK_MINUTES = int(os.environ.get("NEW_ORDER_LOOKBACK_MINUTES", "15"))

# --- "Today only" safety floor ---
# Even if the DB is wiped, the script will NEVER text orders older than the start of
# "today" (in the store's timezone), minus a small grace so orders placed just before
# midnight aren't lost when the cron rolls into the next day.
#
# STORE_TZ: leave blank to auto-detect from the WooCommerce API each run.
#           Set it explicitly (e.g. "America/New_York") to skip the lookup.
STORE_TZ = os.environ.get("STORE_TZ", "").strip()
# Fallback used only if auto-detect fails and STORE_TZ is not set.
STORE_TZ_FALLBACK = os.environ.get("STORE_TZ_FALLBACK", "America/New_York")
# Grace window: how far before local midnight we still allow (catches ~midnight orders).
MIDNIGHT_GRACE_MINUTES = int(os.environ.get("MIDNIGHT_GRACE_MINUTES", "20"))

# Prune texted-order records older than this many days to keep the DB small.
STATE_RETENTION_DAYS = int(os.environ.get("NEW_ORDER_STATE_RETENTION_DAYS", "30"))

# Files (kept next to the script by default)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "new_order_sms.db")
EXCLUDE_FILE = os.path.join(BASE_DIR, "exclude_list.txt")
LOCK_FILE = os.path.join(BASE_DIR, "new_order_sms.lock")
LOG_FILE = os.path.join(BASE_DIR, "new_order_sms.log")  # the ONE log file

# python order_place_sms.py          -> DRY RUN (shows who would get texted, sends nothing)
# python order_place_sms.py --send   -> actually sends the SMS
# python order_place_sms.py --testsend -> send ONE message to the hard-coded test number
SEND_MODE = "--send" in sys.argv
TESTSEND_MODE = "--testsend" in sys.argv

# Hard-coded test recipient — your own number, so --testsend never texts a customer.
TEST_RECIPIENT = {
    "number": "+918392930664",   # your number
    "first": "Shiva",
    "last": "Gupta",
}


# ---- Logging: everything goes to ONE file (and to the console) ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("new_order_sms")


def parse_json(resp):
    """Parse a response as JSON, tolerating a leading UTF-8 BOM and stray
    whitespace. Some WordPress plugins/themes emit a BOM (\\ufeff) before the JSON,
    which makes strict resp.json() fail with 'Unexpected UTF-8 BOM'. Decoding as
    utf-8-sig strips the BOM safely; plain utf-8 otherwise."""
    text = resp.content.decode("utf-8-sig", errors="replace").strip()
    if not text:
        return []
    return json.loads(text)


# ---- SQLite helpers ----
def db_connect():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    # WAL mode = better concurrency and durability; safe for a minute-cron.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sent_orders (
            order_id  TEXT PRIMARY KEY,
            phone     TEXT,
            name      TEXT,
            status    TEXT,          -- 'sent', 'no_phone', 'excluded'
            texted_at TEXT NOT NULL  -- ISO8601 UTC
        );
        """
    )
    # PRIMARY KEY already indexes order_id (O(log n) lookups & inserts).
    # Extra index on texted_at makes pruning fast.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_texted_at ON sent_orders(texted_at);")

    # --- ShipStation phase tracking (added columns; migration-safe) ---
    # ss_order_created:  1 once the order is pushed to ShipStation (V1)
    # ss_stock_deducted: 1 once inventory is decremented (V2)
    # These are SEPARATE from the SMS 'status' so ShipStation sync and SMS never
    # block each other — a no-phone order still gets pushed to ShipStation.
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(sent_orders);")}
    if "ss_order_created" not in existing_cols:
        conn.execute("ALTER TABLE sent_orders ADD COLUMN ss_order_created INTEGER DEFAULT 0;")
    if "ss_stock_deducted" not in existing_cols:
        conn.execute("ALTER TABLE sent_orders ADD COLUMN ss_stock_deducted INTEGER DEFAULT 0;")
    if "ss_detail" not in existing_cols:
        conn.execute("ALTER TABLE sent_orders ADD COLUMN ss_detail TEXT;")
    conn.commit()
    return conn


def ss_state(conn, order_id):
    """Return (ss_order_created, ss_stock_deducted) for an order, or (0,0)."""
    row = conn.execute(
        "SELECT ss_order_created, ss_stock_deducted FROM sent_orders WHERE order_id=? LIMIT 1;",
        (order_id,),
    ).fetchone()
    return (row[0] or 0, row[1] or 0) if row else (0, 0)


def ss_record(conn, order_id, created, deducted, detail):
    """Upsert ShipStation phase flags without disturbing SMS columns.
    Uses INSERT OR IGNORE to guarantee a row exists, then UPDATE the ss_* fields."""
    conn.execute(
        """INSERT OR IGNORE INTO sent_orders (order_id, phone, name, status, texted_at)
           VALUES (?, NULL, NULL, 'ship_only', ?);""",
        (order_id, datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
    )
    conn.execute(
        """UPDATE sent_orders
           SET ss_order_created=?, ss_stock_deducted=?, ss_detail=?
           WHERE order_id=?;""",
        (created, deducted, detail, order_id),
    )
    conn.commit()


def ss_try_claim(conn, order_id):
    """Atomically claim an order for ShipStation creation.

    Returns True only if THIS call won the claim (no prior run had already set
    ss_order_created). Closes the race where two overlapping runs both read
    ss_order_created=0, both POST, and create a DUPLICATE order (the #3643 bug).

    Mechanism: a single UPDATE that flips ss_order_created 0->1 only if it is
    currently 0. SQLite serializes writes, so exactly one concurrent run's UPDATE
    affects the row; the loser sees rowcount 0 and backs off. We claim BEFORE the
    network POST, so a second run can never also POST. If the POST later fails, we
    release the claim (ss_release_claim) so it retries next run."""
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO sent_orders (order_id, phone, name, status, texted_at)
           VALUES (?, NULL, NULL, 'ship_only', ?);""",
        (order_id, now),
    )
    cur = conn.execute(
        """UPDATE sent_orders
           SET ss_order_created=1, ss_detail='claim: creating'
           WHERE order_id=? AND (ss_order_created IS NULL OR ss_order_created=0);""",
        (order_id,),
    )
    conn.commit()
    return cur.rowcount == 1  # True = we won the claim


def ss_release_claim(conn, order_id, detail):
    """Undo a claim if the POST failed, so the order is retried next run."""
    conn.execute(
        "UPDATE sent_orders SET ss_order_created=0, ss_detail=? WHERE order_id=?;",
        (detail, order_id),
    )
    conn.commit()


def already_processed(conn, order_id):
    """True if this order has already been handled for SMS.

    NOTE: a row may exist purely because the ShipStation sync created it first
    (status='ship_only') without any SMS having been sent. So we must check the
    SMS status, not just row existence — otherwise ShipStation creating the row
    would wrongly suppress the SMS. An order counts as SMS-processed only if its
    status is one of the real SMS outcomes."""
    cur = conn.execute(
        "SELECT status FROM sent_orders WHERE order_id = ? LIMIT 1;", (order_id,)
    )
    row = cur.fetchone()
    if not row:
        return False
    return row[0] in ("sent", "no_phone", "excluded")


def record_order(conn, order_id, phone, name, status):
    """Record the SMS outcome. Uses upsert semantics because a row may already
    exist from the ShipStation sync (status='ship_only'); in that case we fill in
    the SMS status/phone/name rather than being ignored. We never downgrade a real
    SMS status back to 'ship_only'."""
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn.execute(
        """
        INSERT INTO sent_orders (order_id, phone, name, status, texted_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
            phone=excluded.phone,
            name=excluded.name,
            status=excluded.status,
            texted_at=excluded.texted_at;
        """,
        (order_id, phone, name, status, now),
    )
    conn.commit()  # durable per-insert, but cheap — only writes the one row


def prune_db(conn):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=STATE_RETENTION_DAYS)) \
        .replace(microsecond=0).isoformat()
    cur = conn.execute("DELETE FROM sent_orders WHERE texted_at < ?;", (cutoff,))
    conn.commit()
    if cur.rowcount:
        log.info("Pruned %d record(s) older than %d days.", cur.rowcount, STATE_RETENTION_DAYS)


def message_for(name):
    first = name.split()[0] if name and name != "(no name)" else "there"
    return (
        f"Hi {first}, thanks for your order with PureX Bio! "
        f"We've received it and it's being processed. "
        f"Please head to purexbio.com accounts page for more details. "
    )


def normalize_phone(raw):
    try:
        if raw is None:
            return None
        digits = re.sub(r"\D", "", str(raw))
        if not digits:
            return None
        if len(digits) == 10:
            digits = "1" + digits
        if len(digits) == 11 and digits.startswith("1"):
            return "+" + digits
        return "+" + digits
    except Exception:
        # Never let a malformed phone value crash the caller.
        return None


def ensure_contact(number, first, last):
    """Create or update the Sendblue contact so it carries a real name.

    The send-message endpoint does NOT reliably set the contact name (it leaves
    'lead (placeholder)'), so we upsert the contact via the SDK first. The param
    names use the SDK's generated body_*_1 form (the '_1' variants are preferred).
    update_if_exists=True means existing placeholder contacts get their name filled in.

    Returns True on success. On any failure we log and return False but do NOT raise —
    a text with a placeholder name is still better than no text at all."""
    try:
        sb.contacts.create(
            number=number,
            body_first_name_1=first,
            body_last_name_1=last,
            body_sendblue_number_1=FROM_NUMBER,
            update_if_exists=True,
        )
        return True
    except Exception as e:
        log.warning("Contact upsert failed for %s (%s) — sending anyway.", number, e)
        return False


def load_exclude_list():
    excluded = set()
    if not os.path.exists(EXCLUDE_FILE):
        log.info("No %s found — no numbers excluded.", os.path.basename(EXCLUDE_FILE))
        return excluded
    with open(EXCLUDE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            norm = normalize_phone(line)
            if norm:
                excluded.add(norm)
    log.info("Loaded %d excluded number(s).", len(excluded))
    return excluded


def _safe_zone(name):
    """Return a tzinfo for a zone name, degrading gracefully if the IANA database
    isn't available (e.g. Windows without the 'tzdata' package installed).

    'UTC' always works via a fixed offset. Other named zones require either the OS
    tz database (Linux/Mac) or `pip install tzdata` (Windows); if unavailable, we
    raise so the caller can fall back."""
    if name.upper() == "UTC":
        return timezone.utc
    return ZoneInfo(name)  # may raise ZoneInfoNotFoundError if tzdata missing


def resolve_store_tz():
    """Return a tzinfo for the store. Uses STORE_TZ if set, else asks the
    WooCommerce API, else falls back to STORE_TZ_FALLBACK, else plain UTC."""
    if STORE_TZ:
        try:
            return _safe_zone(STORE_TZ)
        except Exception:
            log.warning("STORE_TZ=%r couldn't be loaded — trying fallback %s.",
                        STORE_TZ, STORE_TZ_FALLBACK)

    # Auto-detect from WooCommerce general settings.
    try:
        resp = requests.get(
            f"{WC_URL}/wp-json/wc/v3/settings/general",
            auth=WC_AUTH, timeout=30,
        )
        resp.raise_for_status()
        settings = {s.get("id"): s.get("value") for s in parse_json(resp)}
        tz_string = settings.get("woocommerce_timezone_string") or ""
        if tz_string:
            log.info("Detected store timezone: %s", tz_string)
            return _safe_zone(tz_string)
        # Some stores use a raw UTC offset instead of a named zone (e.g. "UTC+0").
        offset = settings.get("woocommerce_timezone_offset")
        if offset not in (None, ""):
            hrs = float(offset)
            log.info("Store uses fixed UTC offset %+g; using that.", hrs)
            return timezone(timedelta(hours=hrs))
    except Exception as e:
        log.warning("Could not auto-detect store timezone (%s).", e)

    # Try the configured fallback name, then finally plain UTC (always available).
    try:
        log.info("Using fallback timezone: %s", STORE_TZ_FALLBACK)
        return _safe_zone(STORE_TZ_FALLBACK)
    except Exception:
        log.warning("Fallback zone unavailable (install 'tzdata' for named zones) — "
                    "using plain UTC.")
        return timezone.utc


def compute_after_floor(store_tz):
    """The earliest order-creation time we'll consider, as a UTC ISO8601 string.

    Two bounds combined, whichever is EARLIER (so we never miss a legitimately new
    order, but also never reach past the start of today):
      - now - LOOKBACK_MINUTES        (normal catch-up window)
      - start-of-today(store_tz) - grace  (the hard 'today only' floor)

    Because the lookback window (~15 min) is always well inside today except right
    after midnight, this normally equals 'now - LOOKBACK'. Only near midnight, or
    after a DB wipe, does the today-floor become the binding limit — capping how far
    back we can ever reach to 'start of today minus grace'.
    """
    now_utc = datetime.now(timezone.utc)

    # Start of today in the store's timezone, converted to UTC.
    now_local = now_utc.astimezone(store_tz)
    start_of_today_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    floor_from_today = (
        start_of_today_local.astimezone(timezone.utc)
        - timedelta(minutes=MIDNIGHT_GRACE_MINUTES)
    )

    floor_from_lookback = now_utc - timedelta(minutes=LOOKBACK_MINUTES)

    # We must not reach past the today-floor, but if the normal lookback is already
    # more recent than the floor, use the lookback (tighter). So: take the LATER of
    # the two as the effective 'after' — that both respects the floor AND avoids
    # scanning more than needed.
    #   - Normal case: lookback (e.g. 15 min ago) is LATER than today-floor -> use lookback.
    #   - Just after midnight / DB wipe: today-floor is LATER -> use today-floor.
    effective = max(floor_from_today, floor_from_lookback)
    return effective.replace(microsecond=0).isoformat()


def fetch_recent_orders(statuses, after_iso):
    orders, page = [], 1
    while True:
        resp = requests.get(
            f"{WC_URL}/wp-json/wc/v3/orders",
            params={
                # FIX: WooCommerce needs statuses as a single comma-separated string.
                # Passing a Python list here sent repeated `status=` params, which the
                # API mishandles (keeps only the last one) — that silently filtered out
                # every order that wasn't the last status in the list.
                "status": ",".join(statuses),
                "after": after_iso,          # ISO8601; WooCommerce filters by created date
                # Interpret `after` as GMT/UTC (we build the floor in UTC), so the
                # window can't drift with store timezone.
                "dates_are_gmt": True,
                "per_page": 100,
                "page": page,
                "orderby": "date",
                "order": "desc",
            },
            auth=WC_AUTH,
            timeout=30,
        )
        resp.raise_for_status()
        batch = parse_json(resp)
        if not batch:
            break
        orders.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return orders


def send_sms(number, content, first_name="", last_name=""):
    headers = {
        "sb-api-key-id": SENDBLUE_API_KEY,
        "sb-api-secret-key": SENDBLUE_API_SECRET,
        "Content-Type": "application/json",
    }
    payload = {
        "number": number,
        "from_number": FROM_NUMBER,
        "content": content,
    }
    # Include the name too (harmless), though the real name-setting is done by
    # ensure_contact() via the SDK before this call.
    if first_name:
        payload["first_name"] = first_name
    if last_name:
        payload["last_name"] = last_name
    resp = requests.post(SEND_URL, json=payload, headers=headers, timeout=30)
    return resp.status_code, resp.text


def run_testsend():
    log.info("TESTSEND — upserting contact + sending ONE message to the test number only.")
    number = normalize_phone(TEST_RECIPIENT["number"])
    # Set the name first (this is what fixes the placeholder), then send.
    ensure_contact(number, TEST_RECIPIENT["first"], TEST_RECIPIENT["last"])
    content = message_for(f"{TEST_RECIPIENT['first']} {TEST_RECIPIENT['last']}")
    code, text = send_sms(number, content,
                          TEST_RECIPIENT["first"], TEST_RECIPIENT["last"])
    log.info("TESTSEND result: HTTP %s", code)
    log.info("Response: %s", text)
    if 200 <= code < 300:
        log.info("OK — check Sendblue dashboard: the contact should show the real name.")
    else:
        log.error("Send did not return 2xx — see response above.")


# =====================================================================
# ShipStation: create order (V1). Stock/fulfillment handled manually by
# the ShipStation team from their dashboard — we only push the order.
#
# IMPORTANT: WooCommerce uses PACK SKUs (e.g. PX-CU50-3 = a 3-vial pack), but
# ShipStation's products use the BASE SKU (PX-CU50) counted in individual vials.
# So each line item is converted:
#     PX-CU50-3  x 1 pack   ->   PX-CU50  x 3 vials
#     PX-TR30-5  x 2 packs  ->   PX-TR30  x 10 vials
# This lets ShipStation match the SKU (allocate stock) and gives pickers the
# correct vial count.
# =====================================================================

def parse_sku(sku):
    """Split a WooCommerce variant SKU into (base_sku, vial_count).

       'PX-RT10-10' -> ('PX-RT10', 10);  'PX-CU50-3' -> ('PX-CU50', 3);
       'PX-RT10-20' -> ('PX-RT10', 20);  'PX-CU50'    -> ('PX-CU50', 1).

    A WooCommerce variant SKU is <base>-<vials>, where <vials> is the trailing
    number after the LAST dash. We treat ANY trailing -<number> as the vial count
    (not a fixed 3/5/10 whitelist), so new variant sizes like -20 work with no
    code change. The greedy (.*) means bases that themselves end in a number
    (PX-RT10, PX-CU50) still split correctly: 'PX-RT10-10' -> base 'PX-RT10',
    vials 10 — because the regex anchors on the FINAL '-<number>'.

    IMPORTANT: this assumes every WooCommerce SKU's trailing '-<number>' is a vial
    count. If you ever add a product whose real base SKU legitimately ends in
    '-<number>' (where that number is NOT vials), it would be mis-split — add an
    explicit exception here for it."""
    if not sku:
        return sku, 1
    m = re.match(r"^(.*)-(\d+)$", sku)
    if m:
        return m.group(1), int(m.group(2))
    return sku, 1


def _ss_addr(node):
    node = node or {}
    name = f"{(node.get('first_name') or '').strip()} {(node.get('last_name') or '').strip()}".strip()
    return {
        "name": name or None,
        "company": node.get("company") or None,
        "street1": node.get("address_1") or None,
        "street2": node.get("address_2") or None,
        "city": node.get("city") or None,
        "state": node.get("state") or None,
        "postalCode": node.get("postcode") or None,
        "country": node.get("country") or None,
        "phone": node.get("phone") or None,
    }


def ss_create_order(order):
    """Create/upsert the order in ShipStation via V1. orderKey makes it idempotent
    on ShipStation's side (re-POSTing the same key updates rather than duplicates).

    Line items are converted from WooCommerce pack SKUs to ShipStation base SKUs
    with vial-count quantities (see module note above). Returns (ok, message)."""
    billing = order.get("billing") if isinstance(order.get("billing"), dict) else {}
    shipping = order.get("shipping") if isinstance(order.get("shipping"), dict) else {}
    ship_to = _ss_addr(shipping if shipping.get("address_1") else billing)
    bill_to = _ss_addr(billing)

    items = []
    for li in (order.get("line_items") or []):
        wc_sku = (li.get("sku") or "")
        base_sku, pack = parse_sku(wc_sku)
        try:
            order_qty = int(li.get("quantity") or 1)
        except (TypeError, ValueError):
            order_qty = 1
        vial_qty = pack * order_qty            # 3-vial pack x1 -> 3 vials

        # Per-vial unit price so the line total still matches (pack price / pack size).
        try:
            line_price = float(li.get("price") or 0)   # WC 'price' is per pack unit
        except (TypeError, ValueError):
            line_price = 0.0
        per_vial_price = round(line_price / pack, 2) if pack else line_price

        items.append({
            "sku": base_sku,                   # ShipStation base SKU (e.g. PX-CU50)
            "name": (li.get("name") or ""),    # keep the readable "GHK-CU - 50mg, 3 Vials"
            "quantity": vial_qty,              # counted in vials
            "unitPrice": per_vial_price,
        })

    body = {
        "orderNumber": str(order.get("number") or order.get("id")),
        "orderKey": f"wc-{order.get('id')}",   # idempotency key
        "orderDate": order.get("date_created_gmt") or order.get("date_created"),
        "orderStatus": "awaiting_shipment",
        "customerEmail": (billing.get("email") or None),
        "billTo": bill_to,
        "shipTo": ship_to,
        "items": items,
    }
    try:
        r = requests.post(f"{SHIP_V1_BASE}/orders/createorder",
                          auth=SHIP_V1_AUTH, json=body, timeout=30)
        ok = 200 <= r.status_code < 300
        return ok, f"HTTP {r.status_code}: {r.text[:180]}"
    except Exception as e:
        return False, f"exception: {e}"


def sync_order_to_shipstation(conn, order, oid, order_number):
    """Push one order to ShipStation (V1), idempotently and race-safe.

    Duplicate prevention (fixes the #3643 double-order bug) has three layers:
      1. Atomic DB claim BEFORE the POST — two overlapping runs can't both proceed.
      2. A pre-POST lookup against ShipStation by orderKey — catches DB-wipe /
         crash-after-post cases.
      3. ShipStation's own orderKey upsert on the POST itself.
    Honors SEND_MODE (dry-run)."""
    if not _SHIP_ENABLED:
        return  # ShipStation creds not configured; silently skip

    created, _ = ss_state(conn, oid)
    if created:
        return  # already pushed (or claimed) — nothing to do

    if not SEND_MODE:
        n_items = len(order.get("line_items") or [])
        log.info("WOULD SHIP      order #%s -> create in ShipStation (%d item(s))", oid, n_items)
        return

    # LAYER 1: atomically claim this order. If we don't win, another run is
    # handling it — back off without posting.
    if not ss_try_claim(conn, oid):
        log.info("SKIP (claimed)  order #%s already being created by another run", oid)
        return

    # (Layer 2 pre-check removed: ShipStation's /orders filters are unreliable and
    #  caused false positives. Duplicate prevention now relies on Layer 1 (atomic
    #  DB claim) + Layer 3 (ShipStation upserts on orderKey during the POST).)

    # LAYER 3: POST (ShipStation upserts on orderKey).
    ok, msg = ss_create_order(order)
    if not ok:
        # Release the claim so a genuine failure retries next run.
        ss_release_claim(conn, oid, f"create failed: {msg}")
        log.error("SHIP create FAILED order #%s: %s", oid, msg)
        return
    ss_record(conn, oid, 1, 1, "order created")
    log.info("=SHIPPED= order #%s created in ShipStation", oid)


def acquire_lock():
    """Prevent two runs overlapping (e.g. a slow run still going when the next
    minute fires). Cross-platform: uses atomic O_CREAT|O_EXCL file creation, which
    works on both Windows and Linux — no fcntl needed.

    A stale lock (left behind by a crash) older than LOCK_STALE_SECONDS is treated
    as dead and reclaimed, so a hard crash won't wedge the script forever."""
    LOCK_STALE_SECONDS = 300  # 5 min; a normal run finishes in seconds

    # If an old lock file is lying around from a crash, clear it.
    if os.path.exists(LOCK_FILE):
        try:
            age = time.time() - os.path.getmtime(LOCK_FILE)
            if age > LOCK_STALE_SECONDS:
                log.warning("Removing stale lock (age %.0fs).", age)
                os.remove(LOCK_FILE)
        except OSError:
            pass

    try:
        # O_EXCL makes this fail if the file already exists — atomic across platforms.
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        log.info("Another run is in progress — exiting.")
        sys.exit(0)

    os.write(fd, str(os.getpid()).encode())
    os.close(fd)

    # Ensure the lock is removed when this process exits (normally or via error).
    atexit.register(lambda: os.path.exists(LOCK_FILE) and os.remove(LOCK_FILE))
    return LOCK_FILE


def main():
    lock = acquire_lock()  # held until process exits
    conn = db_connect()
    prune_db(conn)

    excluded = load_exclude_list()

    store_tz = resolve_store_tz()
    after_iso = compute_after_floor(store_tz)

    try:
        orders = fetch_recent_orders(TARGET_STATUSES, after_iso)
    except requests.RequestException as e:
        log.error("Failed to fetch orders: %s", e)
        conn.close()
        sys.exit(1)

    if not SEND_MODE:
        log.info("*** DRY RUN — no SMS will be sent. Use --send to actually send. ***")

    log.info("Fetched %d order(s) since %s (statuses=%s).",
             len(orders), after_iso, ",".join(TARGET_STATUSES))

    sent = already = skipped_no_phone = skipped_excluded = failed = errored = 0

    # Oldest first so records land in a sensible order.
    # NOTE: sort key is defensive — a missing/none id sorts as 0 rather than crashing.
    for o in sorted(orders, key=lambda x: (x or {}).get("id") or 0):
        # --- Per-order guard: one malformed order must NEVER kill the whole batch.
        # Anything unexpected in a single order is logged and skipped; the loop
        # continues so every other order still gets processed.
        oid = "?"
        try:
            if not isinstance(o, dict):
                errored += 1
                log.warning("SKIP malformed order (not an object): %r", o)
                continue

            oid = str(o.get("id") or "").strip()
            if not oid:
                errored += 1
                log.warning("SKIP order with no id: %r", o)
                continue

            # --- ShipStation sync FIRST, for EVERY order ---
            # Push the order to ShipStation (create only). This is independent of
            # the SMS dedup below: an order must reach ShipStation even if the
            # customer has no phone, is excluded, or was already texted. It has its
            # own flag (ss_order_created) so it's idempotent and never double-creates.
            # Stock deduction / fulfillment is handled manually by the ShipStation team.
            order_number = str(o.get("number") or oid)
            try:
                sync_order_to_shipstation(conn, o, oid, order_number)
            except Exception as e:
                log.exception("SHIP sync error order #%s (SMS continues): %s", oid, e)

            # PRIMARY DEDUP (SMS only): O(log n) index lookup.
            if already_processed(conn, oid):
                already += 1
                continue

            # billing may be missing OR explicitly null — handle both.
            b = o.get("billing")
            if not isinstance(b, dict):
                b = {}

            phone = normalize_phone(b.get("phone"))
            first = (b.get("first_name") or "").strip()
            last = (b.get("last_name") or "").strip()
            name = f"{first} {last}".strip() or "(no name)"

            if not phone:
                skipped_no_phone += 1
                log.info("SKIP no-phone   order #%s (%s)", oid, name)
                record_order(conn, oid, None, name, "no_phone")
                continue

            if phone in excluded:
                skipped_excluded += 1
                log.info("SKIP excluded   order #%s %s (%s)", oid, name, phone)
                record_order(conn, oid, phone, name, "excluded")
                continue

            content = message_for(name)

            if not SEND_MODE:
                log.info("WOULD SEND      order #%s %s (%s)", oid, name, phone)
                continue

            # Set the contact name FIRST (send-message endpoint won't do it reliably),
            # then send the actual text.
            ensure_contact(phone, first, last)

            # --- Send (its own try so a send error doesn't skip recording logic above).
            try:
                code, text = send_sms(phone, content, first, last)
            except Exception as e:  # network, timeout, unexpected SDK/HTTP error
                failed += 1
                log.error("FAILED          order #%s %s (%s) send error: %s",
                          oid, name, phone, e)
                continue  # not recorded -> retried next run

            # FIX: Sendblue returns HTTP 202 (Accepted/QUEUED) on a successful send,
            # not just 200/201. Accept any 2xx so successes aren't mislabeled as
            # failures (which previously left them unrecorded and caused re-texts).
            if 200 <= code < 300:
                sent += 1
                record_order(conn, oid, phone, name, "sent")  # durable immediately
                # Single tagged line — grep '=SENT=' for the clean "who got texted" list.
                log.info("=SENT= order #%s | %s | %s | status=%s",
                         oid, name, phone, o.get("status", "?"))
            else:
                failed += 1
                log.error("FAILED          order #%s %s (%s) HTTP %s: %s",
                          oid, name, phone, code, text)
                # NOTE: not recorded, so a transient failure retries next run.

        except Exception as e:
            # Absolute backstop: any unforeseen error on THIS order is contained here.
            errored += 1
            log.exception("ERROR processing order #%s — skipped: %s", oid, e)
            continue

    try:
        conn.close()
    except Exception:
        pass

    log.info(
        "Done. sent=%d already_sent=%d no_phone=%d excluded=%d failed=%d errored=%d",
        sent, already, skipped_no_phone, skipped_excluded, failed, errored,
    )


if __name__ == "__main__":
    try:
        if TESTSEND_MODE:
            run_testsend()
        else:
            main()
    except SystemExit:
        raise
    except Exception as e:
        log.exception("FATAL: unhandled error in main(): %s", e)
        sys.exit(1)