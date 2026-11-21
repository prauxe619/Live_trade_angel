from __future__ import annotations

import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, jsonify, request, redirect, session, url_for
from werkzeug.security import generate_password_hash, check_password_hash
import secrets, hmac, re, urllib.parse, urllib.request, urllib.error
from contextlib import contextmanager


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║ BLACK — Trading Dashboard API / Audit Server                                ║
# ║ Reads the REAL v11 engine ledger + signal log + optional paper-trader DB.   ║
# ║ This version is designed for full trade/model analysis, not only today's   ║
# ║ dashboard view.                                                             ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "")
if not app.secret_key:
    # Persist a random key locally so sessions survive normal service restarts.
    _secret_file = Path(os.environ.get("BLACK_AUTH_SECRET_FILE", str(Path(__file__).with_name(".black_session_secret"))))
    try:
        _secret_file.parent.mkdir(parents=True, exist_ok=True)
        if _secret_file.exists():
            app.secret_key = _secret_file.read_text().strip()
        else:
            _secret = secrets.token_urlsafe(48)
            _secret_file.write_text(_secret)
            try: _secret_file.chmod(0o600)
            except OSError: pass
            app.secret_key = _secret
    except OSError:
        raise RuntimeError("Set FLASK_SECRET_KEY: unable to create persistent session secret")

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "1").lower() not in {"0", "false", "no"},
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=8),
)
AUTH_DB = Path(os.environ.get("BLACK_AUTH_DB", str(Path(__file__).resolve().parent / "black_auth.db")))


@contextmanager
def _auth_connect():
    AUTH_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(AUTH_DB), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _init_auth_db():
    with _auth_connect() as db:
        db.execute("""CREATE TABLE IF NOT EXISTS auth_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE COLLATE NOCASE,
            phone TEXT UNIQUE,
            password_hash TEXT,
            display_name TEXT NOT NULL DEFAULT 'BLACK Admin',
            google_sub TEXT UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""")
        db.execute("CREATE INDEX IF NOT EXISTS idx_auth_users_phone ON auth_users(phone)")
        admin_email = os.environ.get("BLACK_ADMIN_EMAIL", "").strip().lower()
        admin_password = os.environ.get("BLACK_ADMIN_PASSWORD", "")
        admin_phone = re.sub(r"\D", "", os.environ.get("BLACK_ADMIN_PHONE", "")) or None
        if admin_email and admin_password:
            row = db.execute("SELECT id FROM auth_users WHERE email=?", (admin_email,)).fetchone()
            if not row:
                db.execute("INSERT INTO auth_users(email, phone, password_hash, display_name) VALUES(?,?,?,?)",
                           (admin_email, admin_phone, generate_password_hash(admin_password), "BLACK Admin"))
            elif admin_phone:
                db.execute("UPDATE auth_users SET phone=COALESCE(phone, ?) WHERE email=?", (admin_phone, admin_email))


_init_auth_db()


def _current_user():
    uid = session.get("user_id")
    if not uid: return None
    with _auth_connect() as db:
        return db.execute("SELECT id,email,phone,display_name FROM auth_users WHERE id=? AND is_active=1", (uid,)).fetchone()


def _login_user(user):
    session.clear()
    session.permanent = True
    session["user_id"] = int(user["id"])
    session["auth_at"] = int(time.time())


def _json_error(message, status=400):
    return jsonify({"ok": False, "error": message}), status


LOGIN_HTML = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BLACK | Secure Login</title><style>
:root{color-scheme:dark;--bg:#070a13;--panel:#101728;--line:#26324c;--muted:#91a0bf;--blue:#4388ff;--white:#edf3ff}*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;background:radial-gradient(ellipse at 50% 0,#14264a 0,transparent 55%),var(--bg);font:14px Inter,system-ui;color:var(--white);padding:22px}.wrap{width:min(440px,100%)}.brand{text-align:center;margin-bottom:25px}.mark{font-size:34px;font-weight:900;letter-spacing:9px}.sub{color:var(--muted);font-size:11px;letter-spacing:3px;margin-top:7px}.card{background:linear-gradient(145deg,#121c30,#0c1220);border:1px solid var(--line);border-radius:18px;padding:28px;box-shadow:0 24px 80px #0009}.eyebrow{color:#72a6ff;font-size:10px;letter-spacing:2px;font-weight:800}.h{font-size:23px;font-weight:800;margin:9px 0 5px}.desc{color:var(--muted);font-size:12px;margin-bottom:22px}.tabs{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:18px}.tab{border:1px solid var(--line);background:#0b1220;color:#9eabc5;padding:11px;border-radius:9px;cursor:pointer}.tab.active{color:white;border-color:#4388ff;background:#14284a}.field{margin:13px 0}.field label{display:block;color:#aab7d0;font-size:11px;margin-bottom:7px}.field input{width:100%;background:#080f1d;border:1px solid #2b3854;border-radius:9px;padding:13px;color:white;outline:none}.field input:focus{border-color:#4388ff}.btn{width:100%;border:0;border-radius:9px;background:linear-gradient(90deg,#2563eb,#4388ff);color:white;padding:13px;font-weight:800;cursor:pointer;margin-top:8px}.google{background:#f4f6fb;color:#172033}.divider{display:flex;align-items:center;gap:12px;color:#66738e;font-size:10px;margin:18px 0}.divider:before,.divider:after{content:'';height:1px;background:#29344b;flex:1}.msg{min-height:18px;color:#ff9d9d;font-size:12px;margin-top:12px}.note{font-size:10px;color:#65738f;text-align:center;line-height:1.6;margin:17px 8px 0}.panel{display:none}.panel.active{display:block}.hidden{display:none!important}</style></head><body><main class="wrap"><div class="brand"><div class="mark">BLACK</div><div class="sub">TRADING INTELLIGENCE TERMINAL</div></div><section class="card"><div class="eyebrow">SECURE ACCESS</div><div class="h">Welcome back</div><div class="desc">Sign in to access your private trading dashboard.</div><div class="tabs"><button class="tab active" data-tab="password">Email & Password</button><button class="tab" data-tab="mobile">Mobile OTP</button></div><div id="password" class="panel active"><form id="passwordForm"><div class="field"><label>Email address</label><input name="email" type="email" autocomplete="username" required placeholder="you@example.com"></div><div class="field"><label>Password</label><input name="password" type="password" autocomplete="current-password" required placeholder="Your password"></div><button class="btn">Sign in securely</button></form></div><div id="mobile" class="panel"><div class="field"><label>Mobile number (with country code)</label><input id="phone" type="tel" autocomplete="tel" placeholder="919876543210" maxlength="15"></div><button class="btn" id="sendOtp">Send OTP</button><div id="otpArea" class="hidden"><div class="field"><label>6-digit verification code</label><input id="otp" inputmode="numeric" maxlength="6" placeholder="Enter OTP"></div><button class="btn" id="verifyOtp">Verify & sign in</button></div></div><div class="divider">OR CONTINUE WITH</div><button class="btn google" id="googleBtn">Continue with Google</button><div id="msg" class="msg" role="status"></div></section><div class="note">Protected dashboard · 8-hour secure session<br>Access is restricted to provisioned accounts only.</div></main><script>
const msg=document.getElementById('msg');function say(t,ok=false){msg.style.color=ok?'#72dfa2':'#ff9d9d';msg.textContent=t}document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab,.panel').forEach(x=>x.classList.remove('active'));b.classList.add('active');document.getElementById(b.dataset.tab).classList.add('active');say('')});async function post(url,data){const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const j=await r.json();if(!r.ok||!j.ok)throw Error(j.error||'Request failed');return j}document.getElementById('passwordForm').onsubmit=async e=>{e.preventDefault();say('Signing in…');const f=new FormData(e.target);try{await post('/auth/login/password',{email:f.get('email'),password:f.get('password')});location.href='/'}catch(x){say(x.message)}};document.getElementById('sendOtp').onclick=async()=>{say('Sending verification code…');try{await post('/auth/login/otp/send',{phone:document.getElementById('phone').value});document.getElementById('otpArea').classList.remove('hidden');say('OTP sent. Check your SMS.',true)}catch(x){say(x.message)}};document.getElementById('verifyOtp').onclick=async()=>{say('Verifying…');try{await post('/auth/login/otp/verify',{phone:document.getElementById('phone').value,otp:document.getElementById('otp').value});location.href='/'}catch(x){say(x.message)}};document.getElementById('googleBtn').onclick=()=>location.href='/auth/google';
</script></body></html>'''

@app.before_request
def _require_authentication():
    allowed = {"login", "auth_password_login", "auth_otp_send", "auth_otp_verify", "auth_google", "auth_google_callback", "auth_logout", "health", "static"}
    if request.endpoint in allowed or request.path == "/favicon.ico":
        return None
    if not _current_user():
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Authentication required", "login": "/login"}), 401
        return redirect(url_for("login", next=request.path))
    return None


@app.route("/login")
def login():
    if _current_user(): return redirect("/")
    return LOGIN_HTML


@app.post("/auth/login/password")
def auth_password_login():
    data = request.get_json(silent=True) or {}
    email = str(data.get("email", "")).strip().lower()
    password = str(data.get("password", ""))
    if not email or not password: return _json_error("Enter your email and password.")
    with _auth_connect() as db:
        user = db.execute("SELECT * FROM auth_users WHERE email=? AND is_active=1", (email,)).fetchone()
    if not user or not user["password_hash"] or not check_password_hash(user["password_hash"], password):
        return _json_error("Invalid email or password.", 401)
    _login_user(user)
    return jsonify({"ok": True})


def _msg91_request(url, method="GET", payload=None):
    authkey = os.environ.get("MSG91_AUTHKEY", "").strip()
    if not authkey: raise RuntimeError("Mobile OTP is not configured. Set MSG91_AUTHKEY and MSG91_TEMPLATE_ID.")
    body = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=body, method=method, headers={"authkey": authkey, "Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp: return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="ignore")[:300]
        raise RuntimeError("SMS provider rejected the OTP request: " + detail)


@app.post("/auth/login/otp/send")
def auth_otp_send():
    data = request.get_json(silent=True) or {}
    phone = re.sub(r"\D", "", str(data.get("phone", "")))
    if len(phone) < 10 or len(phone) > 15: return _json_error("Enter a valid mobile number with country code.")
    with _auth_connect() as db:
        user = db.execute("SELECT id FROM auth_users WHERE phone=? AND is_active=1", (phone,)).fetchone()
    if not user: return _json_error("This mobile number is not linked to an authorized account.", 404)
    template = os.environ.get("MSG91_TEMPLATE_ID", "").strip()
    if not template: return _json_error("Mobile OTP is not configured. Set MSG91_TEMPLATE_ID.", 503)
    try:
        query = urllib.parse.urlencode({"template_id": template, "mobile": phone, "otp_length": "6", "otp_expiry": "5"})
        result = _msg91_request("https://control.msg91.com/api/v5/otp?" + query)
        if str(result.get("type", "success")).lower() not in {"success", "successfully", ""}:
            return _json_error("SMS provider could not send the OTP.", 502)
        session["otp_phone"] = phone
        return jsonify({"ok": True})
    except Exception as exc: return _json_error(str(exc), 503)


@app.post("/auth/login/otp/verify")
def auth_otp_verify():
    data = request.get_json(silent=True) or {}
    phone = re.sub(r"\D", "", str(data.get("phone", "")))
    otp = re.sub(r"\D", "", str(data.get("otp", "")))
    if not phone or len(otp) != 6 or not hmac.compare_digest(phone, str(session.get("otp_phone", ""))):
        return _json_error("Request a new OTP and enter the 6-digit code.")
    try:
        query = urllib.parse.urlencode({"mobile": phone, "otp": otp})
        result = _msg91_request("https://control.msg91.com/api/v5/otp/verify?" + query)
        if str(result.get("type", "")).lower() not in {"success", "successfully"}:
            return _json_error("Invalid or expired OTP.", 401)
        with _auth_connect() as db:
            user = db.execute("SELECT * FROM auth_users WHERE phone=? AND is_active=1", (phone,)).fetchone()
        if not user: return _json_error("Account is not authorized.", 403)
        session.pop("otp_phone", None)
        _login_user(user)
        return jsonify({"ok": True})
    except Exception as exc: return _json_error(str(exc), 503)


@app.route("/auth/google")
def auth_google():
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret: return "Google sign-in is not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET.", 503
    try:
        from authlib.integrations.flask_client import OAuth
        oauth = OAuth(app)
        google = oauth.register(name="google", client_id=client_id, client_secret=client_secret,
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"})
        session["google_oauth_ready"] = True
        return google.authorize_redirect(url_for("auth_google_callback", _external=True))
    except ImportError: return "Install Authlib in the dashboard virtualenv: pip install Authlib", 503


@app.route("/auth/google/callback")
def auth_google_callback():
    if not session.pop("google_oauth_ready", False): return redirect("/login")
    try:
        from authlib.integrations.flask_client import OAuth
        oauth = OAuth(app)
        google = oauth.register(name="google", client_id=os.environ["GOOGLE_CLIENT_ID"], client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration", client_kwargs={"scope": "openid email profile"})
        token = google.authorize_access_token()
        info = token.get("userinfo") or google.userinfo()
        email = str(info.get("email", "")).lower()
        if not email or not info.get("email_verified"): return redirect("/login?error=google_email_unverified")
        with _auth_connect() as db:
            user = db.execute("SELECT * FROM auth_users WHERE email=? AND is_active=1", (email,)).fetchone()
        if not user: return "Google account is not provisioned for BLACK dashboard access.", 403
        _login_user(user)
        return redirect("/")
    except Exception as exc:
        app.logger.exception("Google OAuth callback failed")
        return "Google sign-in failed. Check OAuth configuration and callback URL.", 400


@app.post("/auth/logout")
@app.get("/auth/logout")
def auth_logout():
    session.clear()
    return redirect("/login")



# ──────────────────────────────────────────────────────────────────────────────
# PATH CONFIG
# ──────────────────────────────────────────────────────────────────────────────

APP_DIR = Path(__file__).resolve().parent


def _find_project_root() -> Path:
    candidates = [APP_DIR, APP_DIR.parent, Path.cwd()]
    for p in candidates:
        if (p / "data" / "signal_engine_v11").exists():
            return p.resolve()
    return APP_DIR.resolve()


ROOT_DIR = _find_project_root()
DATA_DIR = ROOT_DIR / "data" / "signal_engine_v11"

BLACK_DB = DATA_DIR / "black_trading_ledger.db"
SIGNALS_LOG = DATA_DIR / "signals_log_v11.json"
LEDGER_PATH = ROOT_DIR / "data" / "trades.db"

SNAPSHOTS = {
    "NIFTY": ROOT_DIR / "data" / "live" / "live_snapshot.json",
    "BANKNIFTY": ROOT_DIR / "data" / "live" / "banknifty_live_snapshot.json",
    "FINNIFTY": ROOT_DIR / "data" / "live" / "finnifty_live_snapshot.json",
    "SENSEX": ROOT_DIR / "data" / "live" / "sensex_live_snapshot.json",
}

IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist():
    """Market time. Servers (Railway etc.) usually run in UTC, so never trust datetime.now()."""
    return datetime.now(IST)


INDICES = {
    "NIFTY": {"label": "Nifty 50", "emoji": "🔵", "lot": 65},
    "BANKNIFTY": {"label": "Bank Nifty", "emoji": "🟠", "lot": 30},
    "FINNIFTY": {"label": "Fin Nifty", "emoji": "💜", "lot": 60},
    "SENSEX": {"label": "Sensex", "emoji": "🔴", "lot": 20},
}


# ──────────────────────────────────────────────────────────────────────────────
# LOW-LEVEL HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _read_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _safe_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        v = float(value)
        return default if not math.isfinite(v) else v
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def _parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


def _date_of(record):
    value = record.get("date") or record.get("timestamp") or record.get("entry_time") or record.get("created_at") or ""
    return str(value)[:10]


def _pnl_inr(record):
    for key in ("pnl_inr", "rupee_pnl", "realized_pnl", "net_pnl", "pnl"):
        if record.get(key) is not None:
            return _safe_float(record.get(key))
    return 0.0


def _pnl_pts(record):
    for key in ("pnl_pts", "points_pnl", "points", "realized_points"):
        if record.get(key) is not None:
            return _safe_float(record.get(key))
    return 0.0


def _is_closed(record):
    status = str(record.get("status", "")).lower()
    return status in {"closed", "complete", "completed", "exited", "exit"} or bool(record.get("exit_time") or record.get("exit_price"))


def _normalize_signal(r, source="signal_engine"):
    direction = str(r.get("direction", r.get("side", ""))).upper()
    if direction in {"CALL", "BUY_CALL"}:
        direction = "CE"
    elif direction in {"PUT", "BUY_PUT"}:
        direction = "PE"

    timestamp = r.get("timestamp") or r.get("entry_time") or r.get("time") or ""
    pnl_pts = r.get("pnl_pts")
    pnl_inr = r.get("pnl_inr")

    return {
        "id": str(r.get("signal_id", r.get("trade_id", r.get("id", "")))),
        "signal_id": str(r.get("signal_id", r.get("trade_id", r.get("id", "")))),
        "timestamp": str(timestamp),
        "date": _date_of({"date": r.get("date"), "timestamp": timestamp}),
        "index": str(r.get("index", r.get("symbol", ""))),
        "label": str(r.get("label", r.get("index", r.get("symbol", "")))),
        "direction": direction,
        "side": str(r.get("side", "")),
        "strike": r.get("strike", r.get("strike_price", "")),
        "expiry": r.get("expiry", r.get("expiry_date", "")),
        "entry": _safe_float(r.get("entry_price", r.get("entry", r.get("entry_premium", 0)))),
        "exit": _safe_float(r.get("exit_price", r.get("exit", r.get("exit_premium", 0)))),
        "sl_pts": _safe_float(r.get("sl_pts", r.get("stop_loss_pts", 0))),
        "tp_pts": _safe_float(r.get("tp_pts", r.get("target_pts", 0))),
        "lot": _safe_int(r.get("lot", r.get("lot_size", 0))),
        "prob": _safe_float(r.get("prob", r.get("probability", r.get("confidence", 0)))),
        "regime": str(r.get("regime", "")),
        "spot": _safe_float(r.get("spot", r.get("spot_val", 0))),
        "signal_type": str(r.get("signal_type", r.get("type", ""))),
        "channel": str(r.get("channel", "VIP")),
        "status": str(r.get("status", "open")),
        "outcome": str(r.get("outcome", r.get("exit_reason", ""))),
        "exit_time": str(r.get("exit_time", r.get("closed_at", ""))),
        "pnl_pts": _safe_float(pnl_pts) if pnl_pts is not None else (_safe_float(r.get("exit_price", 0)) - _safe_float(r.get("entry_price", 0)) if _is_closed(r) else None),
        "rupee_pnl": _safe_float(pnl_inr) if pnl_inr is not None else None,
        "pnl_inr": _safe_float(pnl_inr) if pnl_inr is not None else None,
        "trigger": str(r.get("trigger", "fresh")),
        "quantity": _safe_int(r.get("quantity", r.get("lot_size", 0))),
        "sl_price": _safe_float(r.get("sl_price", 0)),
        "tp_price": _safe_float(r.get("tp_price", 0)),
        "trail_trigger": _safe_float(r.get("trail_trigger", 0)),
        "moonbag": bool(r.get("moonbag", False)),
        "peak": _safe_float(r.get("peak", 0)),
        "targets": r.get("targets", []) if isinstance(r.get("targets", []), list) else [],
        "target_progress": r.get("target_progress", {}),
        "source": source,
        "raw": r,
    }


def _read_state():
    if not BLACK_DB.exists():
        return {}
    try:
        with sqlite3.connect(str(BLACK_DB)) as conn:
            row = conn.execute("SELECT state_json FROM engine_state WHERE id = 1").fetchone()
            if row:
                return json.loads(row[0])
    except Exception as e:
        print(f"Dashboard state read error: {e}")
    return {}



def _read_snapshot(index_key):
    path = SNAPSHOTS.get(index_key)

    try:
        from trading_live.shared_store import read_snapshot as read_shared_snapshot

        snap = read_shared_snapshot(index_key, max_age_s=86400)
        if snap:
            return snap
    except Exception as e:
        print(f"PostgreSQL snapshot read error ({index_key}): {e}")

    if not path or not path.exists():
        return {}

    try:
        raw = _read_json(path)
        if isinstance(raw, list):
            for obj in reversed(raw):
                if isinstance(obj, dict) and ("spot" in obj or "spot_val" in obj):
                    return obj
            return raw[0] if raw else {}
        if isinstance(raw, dict) and isinstance(raw.get("data"), list) and raw["data"]:
            last = raw["data"][-1]
            return last if isinstance(last, dict) else raw
        return raw if isinstance(raw, dict) else {}
    except Exception as e:
        print(f"Local snapshot read error ({index_key}): {e}")
        return {}


def _is_market():
    from datetime import time as dt_time
    now = _now_ist()
    return now.weekday() < 5 and dt_time(9, 15) <= now.time() <= dt_time(15, 30)


# ──────────────────────────────────────────────────────────────────────────────
# SIGNAL LOG
# ──────────────────────────────────────────────────────────────────────────────

def _read_signals_log(days=3650):
    if not SIGNALS_LOG.exists():
        return []
    try:
        records = _read_json(SIGNALS_LOG)
        if isinstance(records, dict):
            for key in ("signals", "records", "data", "trades"):
                if isinstance(records.get(key), list):
                    records = records[key]
                    break
        if not isinstance(records, list):
            return []

        cutoff = (datetime.now() - timedelta(days=days)).date()
        out = []
        for r in records:
            if not isinstance(r, dict):
                continue
            date = _date_of(r)
            parsed = _parse_dt(r.get("timestamp") or r.get("entry_time") or r.get("date"))
            if parsed and parsed.date() < cutoff:
                continue
            out.append(_normalize_signal(r, "signal_engine"))

        out.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return out
    except Exception as e:
        print(f"signals_log read error: {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────────
# PAPER TRADER DB
# ──────────────────────────────────────────────────────────────────────────────

def _get_db_trades(days=3650):
    if not LEDGER_PATH.exists():
        return []
    try:
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        with sqlite3.connect(str(LEDGER_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            tables = _sqlite_tables(conn)
            if "trades" not in tables:
                return []
            rows = [dict(r) for r in conn.execute("SELECT * FROM trades WHERE timestamp >= ? ORDER BY timestamp DESC", (since,)).fetchall()]

        out = []
        for t in rows:
            trade_type = str(t.get("type", "")).upper()
            if "EXIT" not in trade_type and "ENTRY" not in trade_type:
                continue
            row = dict(t)
            row["symbol"] = row.get("symbol", "")
            row["status"] = "closed" if "EXIT" in trade_type else "open"
            out.append(_normalize_signal(row, "paper_trader"))
        return out
    except Exception as e:
        print(f"trades.db read error: {e}")
        return []


# ──────────────────────────────────────────────────────────────────────────────
# BLACK DB INSPECTION
# ──────────────────────────────────────────────────────────────────────────────

def _sqlite_tables(conn):
    return [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]


def _black_db_schema():
    if not BLACK_DB.exists():
        return {"exists": False, "tables": []}
    try:
        with sqlite3.connect(str(BLACK_DB)) as conn:
            tables = []
            for name in _sqlite_tables(conn):
                columns = [dict(row) for row in conn.execute(f'PRAGMA table_info("{name}")').fetchall()]
                count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                tables.append({"name": name, "rows": count, "columns": columns})
            return {"exists": True, "path": str(BLACK_DB), "tables": tables}
    except Exception as e:
        return {"exists": True, "path": str(BLACK_DB), "error": str(e), "tables": []}


def _read_black_db_records(limit=10000):
    if not BLACK_DB.exists():
        return []
    result = []
    try:
        with sqlite3.connect(str(BLACK_DB)) as conn:
            conn.row_factory = sqlite3.Row
            for table in _sqlite_tables(conn):
                if table == "engine_state":
                    continue
                try:
                    rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid DESC LIMIT ?', (limit,)).fetchall()
                except Exception:
                    continue
                for row in rows:
                    item = dict(row)
                    item["_db_table"] = table
                    item["_db_source"] = "black_db"
                    result.append(item)
        return result
    except Exception as e:
        print(f"black DB record read error: {e}")
        return []


def _black_db_trade_candidates():
    """
    Attempt to normalize trade-like rows from black DB without assuming a schema.
    Only tables/rows containing trade-like fields are considered.
    """
    rows = _read_black_db_records()
    out = []
    trade_words = ("trade", "signal", "position", "ledger", "order", "execution")
    for r in rows:
        table = str(r.get("_db_table", "")).lower()
        keys = {str(k).lower() for k in r.keys()}
        looks_like_trade = any(w in table for w in trade_words) or bool(keys & {"signal_id", "trade_id", "entry_price", "exit_price", "pnl_inr", "rupee_pnl", "pnl_pts"})
        if not looks_like_trade:
            continue
        n = _normalize_signal(r, "black_db")
        n["db_table"] = r.get("_db_table")
        out.append(n)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# MERGE + DEDUP
# ──────────────────────────────────────────────────────────────────────────────

def _dedup_key(s):
    sid = str(s.get("id") or s.get("signal_id") or "").strip()
    if sid:
        return ("id", sid)
    return (
        "fallback",
        str(s.get("timestamp", "")),
        str(s.get("index", "")),
        str(s.get("direction", "")),
        str(s.get("strike", "")),
        round(_safe_float(s.get("entry", 0)), 4),
    )


def _get_postgres_signal_states(days=3650):
    """Read persisted target/SL/moonbag state keyed by signal_id."""
    try:
        from trading_live.shared_store import _connect

        since = datetime.now() - timedelta(days=days)
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT signal_id, payload
                    FROM engine_signal_states
                    WHERE created_at >= %s
                """, (since,))
                rows = cur.fetchall()

        states = {}
        for sid, payload in rows:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            if isinstance(payload, dict):
                states[str(sid)] = payload
        return states
    except Exception as e:
        print(f"PostgreSQL signal-state read error: {e}")
        return {}


def _get_postgres_paper_trades(days=3650, signal_states=None):
    """Read daily_signal_engine_v11 paper trades from PostgreSQL."""
    try:
        from trading_live.shared_store import _connect

        since = datetime.now() - timedelta(days=days)
        if signal_states is None:
            signal_states = _get_postgres_signal_states(days=days)

        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT signal_id, index_key, direction, strike, expiry,
                           entry_price, quantity, status, exit_price,
                           pnl_rupees, exit_reason, metadata,
                           opened_at, closed_at
                    FROM paper_trades
                    WHERE opened_at >= %s
                    ORDER BY opened_at DESC
                """, (since,))
                rows = cur.fetchall()

        out = []
        for row in rows:
            (sid, idx, direction, strike, expiry, entry, qty, status,
             exit_price, pnl_rupees, reason, metadata, opened_at,
             closed_at) = row

            metadata = metadata if isinstance(metadata, dict) else (
                json.loads(metadata) if metadata else {}
            )

            entry = _safe_float(entry)
            exit_price = _safe_float(exit_price) if exit_price is not None else 0.0
            closed = str(status).lower() == "closed"

            raw = {
                "signal_id": sid,
                "index": idx,
                "direction": direction,
                "strike": float(strike),
                "expiry": expiry,
                "entry_price": entry,
                "exit_price": exit_price,
                "quantity": int(qty),
                "lot": metadata.get("trade_lots", int(qty)),
                "status": status,
                "signal_type": metadata.get("signal_type", ""),
                "timestamp": opened_at.isoformat() if opened_at else "",
                "date": opened_at.date().isoformat() if opened_at else "",
                "exit_time": closed_at.isoformat() if closed_at else "",
                "outcome": reason or "",
                "pnl_pts": round(exit_price - entry, 2) if closed else None,
                "pnl_inr": _safe_float(pnl_rupees) if pnl_rupees is not None else None,
                "rupee_pnl": _safe_float(pnl_rupees) if pnl_rupees is not None else None,
                "prob": _safe_float(metadata.get("prob", 0)),
                "source": "postgres_paper",
                "metadata": metadata,
            }

            # Target ladder and moonbag state are maintained by the engine's
            # engine_signal_states table, not by paper_trades.metadata.
            state = signal_states.get(str(sid), {})
            raw["signal_state"] = state
            raw["targets"] = state.get("targets", [])
            raw["moonbag"] = bool(state.get("moonbag", False))
            raw["peak"] = state.get("peak")
            raw["sl_price"] = state.get("sl_price")
            raw["tp_price"] = state.get("tp_price")
            raw["trail_trigger"] = state.get("trail_trigger")
            raw["target_progress"] = {
                "moonbag": bool(state.get("moonbag", False)),
                "peak": state.get("peak"),
                "sl_price": state.get("sl_price"),
                "tp_price": state.get("tp_price"),
                "targets": state.get("targets", []),
                "outcome": state.get("outcome", reason or ""),
                "exit_price": state.get("exit_price", exit_price),
                "exit_time": state.get("exit_time", ""),
            }

            out.append(_normalize_signal(raw, "postgres_paper"))

        return out

    except Exception as e:
        print(f"PostgreSQL paper trades read error: {e}")
        return []


# ── Target-ladder parsing / trade-detail enrichment ───────────────────────────

_T_NAME = ("name", "label", "target_name", "id", "key")
_T_PRICE = ("price", "target_price", "level", "value", "tp", "px")
_T_HIT = ("hit", "is_hit", "reached", "achieved", "done", "triggered")


def _is_empty(v):
    return v is None or v == "" or v == 0 or v == [] or v == {}


def _norm_target(t, i, peak=0.0):
    """Normalise one target entry from any shape the engine may have persisted."""
    if isinstance(t, dict):
        name = next((str(t[k]) for k in _T_NAME if t.get(k) not in (None, "")), None)
        if name is None and isinstance(t.get("target"), str):
            name = t["target"]
        name = name or f"T{i + 1}"
        price = next((_safe_float(t[k]) for k in _T_PRICE if t.get(k) not in (None, "")), 0.0)
        hit, known = False, False
        for k in _T_HIT:
            if k in t and t[k] is not None:
                hit, known = bool(t[k]), True
                break
        if not known and str(t.get("status", "")).lower() in {"hit", "done", "reached", "achieved"}:
            hit, known = True, True
        return {"name": name, "price": price, "hit": hit, "hit_known": known,
                "hit_time": str(t.get("hit_time") or t.get("hit_at") or "")}
    if isinstance(t, (int, float)) and not isinstance(t, bool):
        price = float(t)
        known = peak > 0 and price > 0
        return {"name": f"T{i + 1}", "price": price, "hit": bool(known and peak >= price),
                "hit_known": known, "hit_time": "", "inferred_from_peak": known}
    return {"name": f"T{i + 1}", "price": 0.0, "hit": False, "hit_known": False, "hit_time": ""}


def _extract_targets(src, peak=0.0):
    """Targets as a list, a name->value dict, or flat keys (t1_price / target1 / tp1 ...)."""
    if not isinstance(src, dict):
        return []
    raw = src.get("targets")
    if isinstance(raw, list) and raw:
        return [_norm_target(t, i, peak) for i, t in enumerate(raw)]
    if isinstance(raw, dict) and raw:
        out = []
        for i, (k, v) in enumerate(raw.items()):
            if isinstance(v, dict):
                v = {"name": k, **v}
            elif isinstance(v, bool):
                v = {"name": k, "hit": v}
            else:
                v = {"name": k, "price": v}
            out.append(_norm_target(v, i, peak))
        return out
    out = []
    for i in range(1, 7):
        for pk in (f"t{i}_price", f"t{i}", f"target{i}", f"target_{i}", f"tp{i}"):
            if src.get(pk) not in (None, "") and not isinstance(src.get(pk), bool):
                hit = src.get(f"t{i}_hit", src.get(f"target{i}_hit", src.get(f"tp{i}_hit")))
                out.append(_norm_target({"name": f"T{i}", "price": src[pk], "hit": hit}, i - 1, peak))
                break
    return out


def _enrich_signal(s, state):
    """
    Fill the gaps the dashboard shows as '—': targets, SL, final target, quantity, peak, moonbag.
    Persisted engine state always wins. Anything we have to work out ourselves is listed in
    s['derived'] so the UI can label it honestly instead of passing it off as recorded data.
    """
    state = state if isinstance(state, dict) else {}
    raw = s.get("raw") if isinstance(s.get("raw"), dict) else {}
    meta = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    derived = []

    entry = _safe_float(s.get("entry"))
    exit_px = _safe_float(s.get("exit"))
    closed = _is_closed(s)
    outcome = str(s.get("outcome") or state.get("outcome") or "").upper()
    idx = str(s.get("index") or "").upper()

    # 1) persisted state, then paper_trades.metadata
    for k in ("sl_price", "tp_price", "peak", "trail_trigger"):
        for src in (state, meta):
            v = _safe_float(src.get(k))
            if v > 0 and _safe_float(s.get(k)) <= 0:
                s[k] = v
    for k in ("sl_pts", "tp_pts"):
        v = _safe_float(meta.get(k)) or _safe_float(state.get(k))
        if v > 0 and _safe_float(s.get(k)) <= 0:
            s[k] = v
    if state.get("moonbag") or meta.get("moonbag"):
        s["moonbag"] = True
    if not s.get("outcome") and state.get("outcome"):
        s["outcome"] = str(state["outcome"])

    # 2) target ladder
    peak = _safe_float(s.get("peak"))
    targets = _extract_targets({"targets": s.get("targets")}, peak) if s.get("targets") else []
    if not targets:
        targets = _extract_targets(state, peak) or _extract_targets(meta, peak) or _extract_targets(raw, peak)

    # 3) quantity
    if _safe_int(s.get("quantity")) <= 0:
        pi, pp = s.get("pnl_inr"), s.get("pnl_pts")
        lot_sz = INDICES.get(idx, {}).get("lot", 0)
        lots = _safe_int(s.get("lot"))
        if pi not in (None, 0) and pp not in (None, 0):
            q = int(round(abs(_safe_float(pi) / _safe_float(pp))))
            if q > 0:
                s["quantity"] = q
                derived.append("quantity")
        if _safe_int(s.get("quantity")) <= 0 and 0 < lots <= 50 and lot_sz:
            s["quantity"] = lots * lot_sz
            derived.append("quantity")

    # 4) SL / final target
    if _safe_float(s.get("sl_price")) <= 0:
        if entry > 0 and _safe_float(s.get("sl_pts")) > 0:
            s["sl_price"] = round(entry - _safe_float(s["sl_pts"]), 2)
            derived.append("sl_price")
        elif closed and "SL" in outcome and exit_px > 0:
            s["sl_price"] = exit_px
            derived.append("sl_price")
    if _safe_float(s.get("tp_price")) <= 0 and entry > 0 and _safe_float(s.get("tp_pts")) > 0:
        s["tp_price"] = round(entry + _safe_float(s["tp_pts"]), 2)
        derived.append("tp_price")

    # 5) no ladder recorded -> show the planned final target, clearly flagged as planned
    if not targets and _safe_float(s.get("tp_price")) > 0:
        targets = [{"name": "Final target", "price": _safe_float(s["tp_price"]),
                    "hit": "TARGET" in outcome, "hit_known": closed, "hit_time": "", "planned": True}]
        derived.append("targets")
    s["targets"] = targets

    # 6) risk metrics (computed, never stored)
    risk_pts = _safe_float(s.get("sl_pts"))
    if risk_pts <= 0 and entry > 0 and _safe_float(s.get("sl_price")) > 0 and "sl_price" not in derived:
        risk_pts = entry - _safe_float(s["sl_price"])
    pp = s.get("pnl_pts")
    s["risk_pts"] = round(risk_pts, 2) if risk_pts > 0 else None
    s["r_multiple"] = round(_safe_float(pp) / risk_pts, 2) if (closed and pp is not None and risk_pts > 0) else None
    s["roi_pct"] = round(_safe_float(pp) / entry * 100, 2) if (closed and pp is not None and entry > 0) else None
    s["mfe_pts"] = round(_safe_float(s.get("peak")) - entry, 2) if (_safe_float(s.get("peak")) > 0 and entry > 0) else None
    t0, t1 = _parse_dt(s.get("timestamp")), _parse_dt(s.get("exit_time"))
    mins = (t1 - t0).total_seconds() / 60 if (t0 and t1) else None
    s["hold_minutes"] = round(mins, 1) if (mins is not None and 0 <= mins <= 60 * 24 * 7) else None

    s["derived"] = derived
    s["state_found"] = bool(state)
    s["target_progress"] = {
        "moonbag": bool(s.get("moonbag")),
        "peak": s.get("peak") or None,
        "sl_price": s.get("sl_price") or None,
        "tp_price": s.get("tp_price") or None,
        "targets": targets,
        "outcome": s.get("outcome", ""),
        "exit_price": exit_px or None,
        "exit_time": s.get("exit_time", ""),
    }
    return s


def _merge_records(win, lose):
    """Keep the higher-priority record but fill its blanks from the lower-priority copy of the same trade."""
    out = dict(win)
    for k, v in lose.items():
        if k in ("source", "raw", "sources"):
            continue
        if _is_empty(out.get(k)) and not _is_empty(v):
            out[k] = v
    out["sources"] = sorted(set((win.get("sources") or [win.get("source")]) + (lose.get("sources") or [lose.get("source")])))
    return out


def _get_all_signals():
    states = _get_postgres_signal_states(days=3650)
    sources = [
        _get_postgres_paper_trades(days=3650, signal_states=states),
        _read_signals_log(days=3650),
        _get_db_trades(days=3650),
        _black_db_trade_candidates(),
    ]
    source_priority = {"postgres_paper": 4, "signal_engine": 3, "paper_trader": 2, "black_db": 1}

    groups = defaultdict(list)
    for source_rows in sources:
        for s in source_rows:
            groups[_dedup_key(s)].append(s)

    signals = []
    for rows in groups.values():
        rows.sort(key=lambda r: source_priority.get(r.get("source"), 0), reverse=True)
        merged = rows[0]
        merged["sources"] = [merged.get("source")]
        for other in rows[1:]:
            merged = _merge_records(merged, other)
        signals.append(merged)

    # Today's state signal is a display fallback only; it is never treated as a closed trade.
    today = _now_ist().strftime("%Y-%m-%d")
    if not any(s.get("date") == today for s in signals):
        state = _read_state()
        best = state.get("best_signal")
        if isinstance(best, dict) and best.get("index"):
            signals.insert(0, _normalize_signal({
                "signal_id": f"state_{best.get('index')}_{today}",
                "timestamp": _now_ist().strftime("%Y-%m-%d %H:%M"),
                "date": today,
                "index": best.get("index", ""),
                "label": INDICES.get(best.get("index", ""), {}).get("label", ""),
                "direction": best.get("direction", ""),
                "strike": best.get("strike", ""),
                "entry_price": best.get("entry_premium", 0),
                "sl_pts": best.get("sl_pts", 0),
                "tp_pts": best.get("tp_pts", 0),
                "lot": INDICES.get(best.get("index", ""), {}).get("lot", 0),
                "prob": best.get("prob", 0),
                "signal_type": best.get("signal_type", "SNIPER"),
                "channel": "VIP",
                "status": "today_best",
                "trigger": "SNIPER",
            }, "state_fallback"))

    for s in signals:
        _enrich_signal(s, states.get(str(s.get("signal_id") or s.get("id") or ""), {}))
        s.pop("raw", None)  # keeps /api payloads small and always JSON-serialisable

    signals.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
    return signals


# ──────────────────────────────────────────────────────────────────────────────
# ANALYTICS
# ──────────────────────────────────────────────────────────────────────────────

def _closed(signals):
    return [s for s in signals if _is_closed(s) and (_pnl_inr(s) != 0 or _pnl_pts(s) != 0 or s.get("pnl_inr") is not None or s.get("pnl_pts") is not None)]


def _profit_factor(rows):
    gross_win = sum(max(0, _pnl_inr(s)) for s in rows)
    gross_loss = sum(abs(min(0, _pnl_inr(s))) for s in rows)
    return round(gross_win / gross_loss, 4) if gross_loss else (999.0 if gross_win else 0.0)


def _max_drawdown(rows):
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for s in sorted(rows, key=lambda x: str(x.get("timestamp", ""))):
        running += _pnl_inr(s)
        peak = max(peak, running)
        max_dd = min(max_dd, running - peak)
    return round(max_dd, 2)


def _bucket_stats(rows, key_func):
    groups = defaultdict(list)
    for s in rows:
        groups[str(key_func(s))].append(s)
    out = {}
    for key, items in groups.items():
        wins = [s for s in items if _pnl_inr(s) > 0]
        losses = [s for s in items if _pnl_inr(s) < 0]
        net = sum(_pnl_inr(s) for s in items)
        out[key] = {
            "trades": len(items),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(items) * 100, 2) if items else 0,
            "gross_win": round(sum(_pnl_inr(s) for s in wins), 2),
            "gross_loss": round(abs(sum(_pnl_inr(s) for s in losses)), 2),
            "net_pnl": round(net, 2),
            "profit_factor": _profit_factor(items),
            "expectancy": round(net / len(items), 2) if items else 0,
            "max_drawdown": _max_drawdown(items),
        }
    return dict(sorted(out.items(), key=lambda kv: kv[1]["net_pnl"], reverse=True))


def _compute_target_analytics(signals):
    """Count persisted target hits and moonbag trades without inferring missing data."""
    closed_or_open = [s for s in signals if any(not t.get("planned") for t in (s.get("targets") or []))]
    target_counts = defaultdict(lambda: {"hit": 0, "not_hit": 0, "known": 0})
    moonbag_count = 0
    moonbag_open = 0

    for s in closed_or_open:
        progress = s.get("target_progress") or {}
        if bool(s.get("moonbag") or progress.get("moonbag")):
            moonbag_count += 1
            if str(s.get("status", "")).lower() == "open":
                moonbag_open += 1

        targets = s.get("targets") or progress.get("targets") or []
        if not isinstance(targets, list):
            continue
        for target in targets:
            if not isinstance(target, dict) or target.get("planned") or target.get("hit_known") is False:
                continue
            name = str(target.get("name") or target.get("label") or target.get("target") or "Unnamed")
            bucket = target_counts[name]
            bucket["known"] += 1
            if bool(target.get("hit")):
                bucket["hit"] += 1
            else:
                bucket["not_hit"] += 1

    return {
        "trades_with_target_state": len(closed_or_open),
        "moonbag_trades": moonbag_count,
        "open_moonbag_trades": moonbag_open,
        "targets": dict(target_counts),
        "note": "Counts include only trades with persisted engine_signal_states data."
    }


def _compute_stats(signals):
    closed = _closed(signals)
    wins = [s for s in closed if _pnl_inr(s) > 0]
    losses = [s for s in closed if _pnl_inr(s) < 0]
    today = _now_ist().strftime("%Y-%m-%d")
    today_closed = [s for s in closed if s.get("date") == today]
    net = sum(_pnl_inr(s) for s in closed)

    best = max(closed, key=_pnl_inr) if closed else None
    worst = min(closed, key=_pnl_inr) if closed else None

    return {
        "total_signals": len(signals),
        "total_trades": len(closed),
        "open_count": sum(1 for s in signals if str(s.get("status", "")).lower() in {"open", "today_best"}),
        "wins": len(wins),
        "losses": len(losses),
        "flat": len(closed) - len(wins) - len(losses),
        "win_rate": round(len(wins) / len(closed) * 100, 2) if closed else 0,
        "total_pnl": round(net, 2),
        "gross_win": round(sum(_pnl_inr(s) for s in wins), 2),
        "gross_loss": round(abs(sum(_pnl_inr(s) for s in losses)), 2),
        "profit_factor": _profit_factor(closed),
        "expectancy": round(net / len(closed), 2) if closed else 0,
        "avg_win": round(sum(_pnl_inr(s) for s in wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(_pnl_inr(s) for s in losses) / len(losses), 2) if losses else 0,
        "best_trade": best,
        "worst_trade": worst,
        "max_drawdown": _max_drawdown(closed),
        "today_trades": len(today_closed),
        "today_pnl": round(sum(_pnl_inr(s) for s in today_closed), 2),
    }


def _compute_streaks(signals):
    closed = sorted(_closed(signals), key=lambda x: str(x.get("timestamp", "")))
    if not closed:
        return {"current_streak": 0, "streak_type": "—", "max_win_streak": 0, "max_loss_streak": 0}

    current_type = "W" if _pnl_inr(closed[-1]) > 0 else "L" if _pnl_inr(closed[-1]) < 0 else "F"
    current = 1
    for s in reversed(closed[:-1]):
        t = "W" if _pnl_inr(s) > 0 else "L" if _pnl_inr(s) < 0 else "F"
        if t == current_type:
            current += 1
        else:
            break

    max_win = max_loss = 0
    cur_win = cur_loss = 0
    for s in closed:
        p = _pnl_inr(s)
        if p > 0:
            cur_win += 1
            cur_loss = 0
            max_win = max(max_win, cur_win)
        elif p < 0:
            cur_loss += 1
            cur_win = 0
            max_loss = max(max_loss, cur_loss)
        else:
            cur_win = cur_loss = 0

    return {
        "current_streak": current,
        "streak_type": current_type,
        "max_win_streak": max_win,
        "max_loss_streak": max_loss,
    }


def _advanced_quant(signals):
    closed = _closed(signals)

    def hour(s):
        dt = _parse_dt(s.get("timestamp"))
        return f"{dt.hour:02d}" if dt else "unknown"

    def prob_bucket(s):
        p = abs(_safe_float(s.get("prob")))
        p = p * 100 if p <= 1 else p
        if p < 50: return "<50%"
        if p < 55: return "50-55%"
        if p < 60: return "55-60%"
        if p < 65: return "60-65%"
        if p < 70: return "65-70%"
        return "70%+"

    return {
        "by_index": _bucket_stats(closed, lambda s: str(s.get("index") or "UNKNOWN").upper()),
        "by_direction": _bucket_stats(closed, lambda s: str(s.get("direction") or "UNKNOWN").upper()),
        "by_signal_type": _bucket_stats(closed, lambda s: str(s.get("signal_type") or "UNKNOWN").upper()),
        "by_trigger": _bucket_stats(closed, lambda s: str(s.get("trigger") or "fresh").lower()),
        "by_outcome": _bucket_stats(closed, lambda s: str(s.get("outcome") or "UNKNOWN").upper()),
        "by_regime": _bucket_stats(closed, lambda s: str(s.get("regime") or "UNKNOWN").upper()),
        "by_hour": _bucket_stats(closed, hour),
        "by_probability": _bucket_stats(closed, prob_bucket),
        "by_date": _bucket_stats(closed, lambda s: s.get("date") or "UNKNOWN"),
    }


def _quant_data(signals):
    closed = _closed(signals)
    hour_map = {h: 0 for h in range(9, 16)}
    trigger_stats = {"reversal": {"pnl": 0, "count": 0}, "fresh": {"pnl": 0, "count": 0}}
    outcome_stats = {"target": {"pnl": 0, "count": 0}, "sl": {"pnl": 0, "count": 0}, "reversal": {"pnl": 0, "count": 0}, "other": {"pnl": 0, "count": 0}}
    target_levels = {"t1_or_trailed": 0, "final_target": 0, "sl_hit": 0, "reversal_exit": 0, "other_exit": 0}

    for s in closed:
        pnl = _pnl_inr(s)
        dt = _parse_dt(s.get("timestamp"))
        if dt and dt.hour in hour_map:
            hour_map[dt.hour] += pnl

        trigger = str(s.get("trigger", "fresh")).lower()
        bucket = "reversal" if trigger == "reversal" else "fresh"
        trigger_stats[bucket]["pnl"] += pnl
        trigger_stats[bucket]["count"] += 1

        outcome = str(s.get("outcome", "")).upper()
        if "TARGET" in outcome:
            outcome_stats["target"]["pnl"] += pnl
            outcome_stats["target"]["count"] += 1
            target_levels["final_target"] += 1
        elif "TRAILED" in outcome or ("SL" in outcome and pnl > 0):
            outcome_stats["target"]["pnl"] += pnl
            outcome_stats["target"]["count"] += 1
            target_levels["t1_or_trailed"] += 1
        elif "SL" in outcome:
            outcome_stats["sl"]["pnl"] += pnl
            outcome_stats["sl"]["count"] += 1
            target_levels["sl_hit"] += 1
        elif "REVERSAL" in outcome:
            outcome_stats["reversal"]["pnl"] += pnl
            outcome_stats["reversal"]["count"] += 1
            target_levels["reversal_exit"] += 1
        else:
            outcome_stats["other"]["pnl"] += pnl
            outcome_stats["other"]["count"] += 1
            target_levels["other_exit"] += 1

    for d in (trigger_stats, outcome_stats):
        for item in d.values():
            item["pnl"] = round(item["pnl"], 2)

    return {
        "hourly_pnl": {str(k): round(v, 2) for k, v in hour_map.items()},
        "trigger_stats": trigger_stats,
        "outcome_stats": outcome_stats,
        "target_levels": target_levels,
    }


# ──────────────────────────────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    html_path = APP_DIR / "dashboard.html"
    if not html_path.exists():
        html_path = ROOT_DIR / "dashboard.html"
    if not html_path.exists():
        return "dashboard.html not found", 404
    return html_path.read_text(encoding="utf-8")


@app.route("/analytics")
def analytics():
    html_path = APP_DIR / "analytics.html"
    if not html_path.exists():
        html_path = ROOT_DIR / "analytics.html"
    if not html_path.exists():
        return "analytics.html not found", 404
    return html_path.read_text(encoding="utf-8")


@app.route("/api/state")
def api_state():
    state = _read_state()
    spots = {}
    vix = 15.0

    for k in INDICES:
        snap = _read_snapshot(k)
        spots[k] = {
            "spot": _safe_float(snap.get("spot", snap.get("spot_val", 0))),
            "pcr": _safe_float(snap.get("pcr", 1.0)),
            "max_pain": _safe_float(snap.get("max_pain", 0)),
            "net_gex": _safe_float(snap.get("net_gex", 0)),
            "regime": snap.get("regime", ""),
            "prob": _safe_float(snap.get("prob", 0)),
            "timestamp": snap.get("timestamp", ""),
        }
        v = _safe_float(snap.get("vix", 0))
        if v > 0:
            vix = v

    return jsonify({
        "engine_date": state.get("date", ""),
        "day_signal_count": state.get("day_signal_count", 0),
        "week_signal_count": state.get("week_signal_count", 0),
        "free_sent_today": state.get("free_sent_today", False),
        "swing_fired_today": state.get("swing_fired_today", False),
        "best_signal": state.get("best_signal"),
        "last_sniper": state.get("last_sniper", {}),
        "active_position": state.get("active_position"),
        "fired": state.get("fired", []),
        "last_regime": state.get("last_regime", ""),
        "spots": spots,
        "vix": round(vix, 2),
        "server_time": datetime.now().strftime("%H:%M:%S"),
        "signals_log_exists": SIGNALS_LOG.exists(),
        "signals_log_count": len(_read_signals_log(days=3650)),
        "black_db_exists": BLACK_DB.exists(),
    })


@app.route("/api/trades")
def api_trades():
    signals = _get_all_signals()
    stats = _compute_stats(signals)
    stats.update(_compute_streaks(signals))
    limit = max(0, _safe_int(request.args.get("limit", 0)))
    offset = max(0, _safe_int(request.args.get("offset", 0)))
    page = signals[offset:offset + limit] if limit else signals

    return jsonify({
        "signals": page,
        "total_records": len(signals),
        "offset": offset,
        "limit": limit or len(signals),
        "stats": stats,
        "advanced": _advanced_quant(signals),
        "target_analytics": _compute_target_analytics(signals),
        "source_info": {
            "root_dir": str(ROOT_DIR),
            "signals_log": str(SIGNALS_LOG),
            "signals_log_exists": SIGNALS_LOG.exists(),
            "black_db": str(BLACK_DB),
            "black_db_exists": BLACK_DB.exists(),
            "trades_db": str(LEDGER_PATH),
            "trades_db_exists": LEDGER_PATH.exists(),
        },
    })


@app.route("/api/signals")
def api_signals():
    date_filter = request.args.get("date", "")
    status_filter = request.args.get("status", "all").lower()
    days = max(1, _safe_int(request.args.get("days", 3650), 3650))
    signals = _get_all_signals()

    if date_filter:
        signals = [s for s in signals if s["date"] == date_filter]
    if status_filter != "all":
        signals = [s for s in signals if str(s["status"]).lower() == status_filter]

    by_date = defaultdict(list)
    for s in signals:
        by_date[s["date"]].append(s)

    date_summaries = []
    for d, sigs in sorted(by_date.items(), reverse=True):
        closed = _closed(sigs)
        wins = sum(1 for s in closed if _pnl_inr(s) > 0)
        date_summaries.append({
            "date": d,
            "total": len(sigs),
            "open": sum(1 for s in sigs if str(s["status"]).lower() == "open"),
            "closed": len(closed),
            "wins": wins,
            "losses": sum(1 for s in closed if _pnl_inr(s) < 0),
            "win_rate": round(wins / len(closed) * 100, 2) if closed else 0,
            "total_pnl": round(sum(_pnl_inr(s) for s in closed), 2),
            "profit_factor": _profit_factor(closed),
        })

    return jsonify({"signals": signals, "total": len(signals), "date_summaries": date_summaries})


@app.route("/api/analytics")
def api_analytics():
    signals = _get_all_signals()
    return jsonify({
        "stats": _compute_stats(signals),
        "streaks": _compute_streaks(signals),
        "quant": _quant_data(signals),
        "advanced": _advanced_quant(signals),
        "target_analytics": _compute_target_analytics(signals),
    })


@app.route("/api/quant_data")
def api_quant_data():
    return jsonify(_quant_data(_get_all_signals()))


@app.route("/api/live")
def api_live():
    state = _read_state()
    spots = {}
    vix = 15.0

    for k in INDICES:
        snap = _read_snapshot(k)
        spots[k] = {
            "spot": _safe_float(snap.get("spot", snap.get("spot_val", 0))),
            "pcr": _safe_float(snap.get("pcr", 0)),
            "net_gex": _safe_float(snap.get("net_gex", 0)),
            "regime": snap.get("regime", ""),
            "prob": _safe_float(snap.get("prob", 0)),
            "timestamp": snap.get("timestamp", ""),
        }
        v = _safe_float(snap.get("vix", 0))
        if v > 0:
            vix = v

    open_signals = [s for s in _get_all_signals() if str(s.get("status", "")).lower() == "open"]

    return jsonify({
        "spots": spots,
        "vix": round(vix, 2),
        "signals_today": state.get("day_signal_count", 0),
        "swing_on": state.get("swing_fired_today", False),
        "server_time": datetime.now().strftime("%H:%M:%S"),
        "is_market": _is_market(),
        "open_positions": len(open_signals),
        "open_signals": open_signals[:50],
    })


@app.route("/api/debug")
def api_debug():
    paths = {"signals_log": SIGNALS_LOG, "black_db": BLACK_DB, "trades_db": LEDGER_PATH}
    paths.update({f"snap_{k}": v for k, v in SNAPSHOTS.items()})
    info = {"root_dir": str(ROOT_DIR), "app_dir": str(APP_DIR), "paths": {}, "black_db_schema": _black_db_schema()}

    for name, path in paths.items():
        if path.exists():
            stat = path.stat()
            info["paths"][name] = {
                "path": str(path),
                "exists": True,
                "size_kb": round(stat.st_size / 1024, 1),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "age_min": round((time.time() - stat.st_mtime) / 60, 1),
            }
        else:
            info["paths"][name] = {"path": str(path), "exists": False}

    info["signal_log_records"] = len(_read_signals_log(days=3650))
    info["merged_trade_records"] = len(_get_all_signals())
    return jsonify(info)


@app.route("/api/db_inspect")
def api_db_inspect():
    """
    Full SQLite audit endpoint.
    Use /api/db_inspect?limit=1000 while developing.
    It exposes table names, columns, row counts and raw rows from black_db.
    """
    limit = min(max(_safe_int(request.args.get("limit", 500), 500), 1), 10000)
    return jsonify({
        "black_db": _black_db_schema(),
        "records": _read_black_db_records(limit=limit),
    })


@app.route("/api/trade_ledger")
def api_trade_ledger():
    """
    Machine-friendly full closed-trade ledger for later model improvement.
    Supports ?date=YYYY-MM-DD, ?index=NIFTY, ?type=SNIPER, ?direction=CE,
    ?status=closed, ?min_prob=0.60, ?limit=1000, ?offset=0.
    """
    signals = _get_all_signals()

    date_filter = request.args.get("date", "").strip()
    index_filter = request.args.get("index", "").strip().upper()
    type_filter = request.args.get("type", "").strip().upper()
    direction_filter = request.args.get("direction", "").strip().upper()
    status_filter = request.args.get("status", "").strip().lower()
    min_prob = request.args.get("min_prob", "").strip()

    if date_filter:
        signals = [s for s in signals if s.get("date") == date_filter]
    if index_filter:
        signals = [s for s in signals if str(s.get("index", "")).upper() == index_filter]
    if type_filter:
        signals = [s for s in signals if str(s.get("signal_type", "")).upper() == type_filter]
    if direction_filter:
        signals = [s for s in signals if str(s.get("direction", "")).upper() == direction_filter]
    if status_filter:
        signals = [s for s in signals if str(s.get("status", "")).lower() == status_filter]
    if min_prob:
        p = _safe_float(min_prob)
        signals = [s for s in signals if abs(_safe_float(s.get("prob"))) >= (p if p <= 1 else p / 100)]

    limit = min(max(_safe_int(request.args.get("limit", 0), 0), 0), 10000)
    offset = max(0, _safe_int(request.args.get("offset", 0), 0))
    rows = signals[offset:offset + limit] if limit else signals

    ledger = []
    for s in rows:
        ledger.append({
            "id": s.get("id"),
            "timestamp": s.get("timestamp"),
            "date": s.get("date"),
            "index": s.get("index"),
            "direction": s.get("direction"),
            "signal_type": s.get("signal_type"),
            "channel": s.get("channel"),
            "trigger": s.get("trigger"),
            "regime": s.get("regime"),
            "prob": s.get("prob"),
            "strike": s.get("strike"),
            "expiry": s.get("expiry"),
            "spot": s.get("spot"),
            "entry": s.get("entry"),
            "exit": s.get("exit"),
            "sl_pts": s.get("sl_pts"),
            "tp_pts": s.get("tp_pts"),
            "pnl_pts": s.get("pnl_pts"),
            "rupee_pnl": _pnl_inr(s),
            "outcome": s.get("outcome"),
            "status": s.get("status"),
            "exit_time": s.get("exit_time"),
            "source": s.get("source"),
            "quantity": s.get("quantity"),
            "sl_price": s.get("sl_price"),
            "tp_price": s.get("tp_price"),
            "peak": s.get("peak"),
            "moonbag": s.get("moonbag"),
            "targets": s.get("targets"),
            "r_multiple": s.get("r_multiple"),
            "roi_pct": s.get("roi_pct"),
            "hold_minutes": s.get("hold_minutes"),
            "derived": s.get("derived"),
        })

    return jsonify({"total": len(signals), "offset": offset, "limit": limit or len(signals), "trades": ledger})


@app.route("/health")
def health():
    # Keep this public probe intentionally minimal; do not disclose filesystem paths.
    return jsonify({"ok": True, "server_time": datetime.now().isoformat(timespec="seconds")})


if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║   BLACK Trading Dashboard  — FULL AUDIT / MODEL LAB                ║")
    print("║   http://localhost:5000                                            ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print(f"║  ROOT       : {str(ROOT_DIR):<55} ║")
    print(f"║  SIGNAL LOG : {'✅ ' + str(SIGNALS_LOG) if SIGNALS_LOG.exists() else '⚠️  MISSING':<55} ║")
    print(f"║  BLACK DB   : {'✅ ' + str(BLACK_DB) if BLACK_DB.exists() else '⚠️  MISSING':<55} ║")
    print(f"║  TRADES DB  : {'✅ ' + str(LEDGER_PATH) if LEDGER_PATH.exists() else '⚠️  OPTIONAL/MISSING':<55} ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print("║  /api/trades       → ALL merged trades + advanced analytics        ║")
    print("║  /api/trade_ledger → clean full trade ledger                      ║")
    print("║  /api/analytics    → model-improvement analytics                   ║")
    print("║  /api/db_inspect   → BLACK SQLite tables + raw records             ║")
    print("║  /api/debug        → file/database health                           ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    app.run(host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8080")), debug=False, threaded=True, use_reloader=False)