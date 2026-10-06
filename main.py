"""ZERO7 Crash — server-authoritative engine (FastAPI, single process, single worker)."""
import asyncio, base64, hashlib, hmac, json, logging, math, os, re, secrets, time, uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation, ROUND_DOWN

import aiomysql
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("crash")

# ---------------- config ----------------
GAME_TOKEN = os.getenv("GAME_UNDERBOARD_TOKEN", "")
PUBLIC_WS_URL = os.getenv("PUBLIC_WS_URL", "")
BETTING_MS = int(os.getenv("CRASH_BETTING_MS", "5000"))
CRASH_MS = int(os.getenv("CRASH_CRASH_MS", "2500"))
MIN_BET = Decimal(os.getenv("CRASH_MIN_BET", "1"))
MAX_BET = Decimal(os.getenv("CRASH_MAX_BET", "100000"))
GROWTH_K = float(os.getenv("CRASH_GROWTH_K", "0.00006"))  # m(t)=exp(K*t_ms)
HOUSE_EDGE_MOD = 33  # 1/33 instant 1.00x; measured RTP ~96.4% (house edge ~3.6%) - tune before launch
CURRENCIES = ("xbite", "gram", "stars")

def _ident(name, default):
    v = os.getenv(name, default)
    if not re.fullmatch(r"[A-Za-z0-9_]+", v):
        raise RuntimeError(f"bad identifier in {name}")
    return v

# ADAPTER to the existing BITE balance storage. ASSUMPTION: one users table with 3 balance columns.
USERS_TABLE = _ident("USERS_TABLE", "users")
USERS_ID_COL = _ident("USERS_ID_COL", "id")
USERS_TG_COL = _ident("USERS_TG_COL", "telegram_id")
BAL_COL = {
    "xbite": _ident("COL_XBITE", "xbite_balance"),
    "gram": _ident("COL_GRAM", "gram_balance"),
    "stars": _ident("COL_STARS", "stars_balance"),
}

D = Decimal
Q2, Q4 = D("0.01"), D("0.0001")
now_ms = lambda: int(time.time() * 1000)
num = lambda x: None if x is None else float(x)

class ApiError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code

pool: aiomysql.Pool = None
round_lock = asyncio.Lock()      # serialises crash vs cashout inside this process
clients: dict = {}               # WebSocket -> internal user_id

# ---------------- db ----------------
@asynccontextmanager
async def tx():
    async with pool.acquire() as c:
        try:
            yield c
            await c.commit()
        except BaseException:
            await c.rollback()
            raise

SCHEMA = [
"""CREATE TABLE IF NOT EXISTS crash_rounds (
  round_id VARCHAR(40) PRIMARY KEY, state VARCHAR(12) NOT NULL,
  created_at BIGINT NOT NULL, started_at BIGINT NULL, crashed_at BIGINT NULL,
  betting_ends_at BIGINT NOT NULL, crash_point DECIMAL(10,2) NULL,
  server_seed CHAR(64) NOT NULL, server_seed_hash CHAR(64) NOT NULL,
  client_seed VARCHAR(64) NOT NULL, nonce BIGINT NOT NULL,
  UNIQUE KEY uq_nonce (nonce), KEY ix_state (state), KEY ix_created (created_at)
) ENGINE=InnoDB""",
"""CREATE TABLE IF NOT EXISTS crash_bets (
  bet_id VARCHAR(40) PRIMARY KEY, round_id VARCHAR(40) NOT NULL, user_id BIGINT UNSIGNED NOT NULL,
  username VARCHAR(64) NOT NULL DEFAULT '', amount DECIMAL(18,2) NOT NULL,
  currency VARCHAR(8) NOT NULL, status VARCHAR(12) NOT NULL,
  auto_cashout DECIMAL(10,2) NULL, cashout_multiplier DECIMAL(10,2) NULL,
  payout DECIMAL(18,2) NULL, placed_at BIGINT NOT NULL, cashed_out_at BIGINT NULL,
  idempotency_key VARCHAR(64) NOT NULL, cashout_key VARCHAR(64) NULL,
  UNIQUE KEY uq_user_idem (user_id, idempotency_key),
  UNIQUE KEY uq_user_cashkey (user_id, cashout_key),
  UNIQUE KEY uq_round_user (round_id, user_id),
  KEY ix_round_status (round_id, status), KEY ix_user_placed (user_id, placed_at)
) ENGINE=InnoDB""",
]

async def q(cur, sql, args=()):
    await cur.execute(sql, args)
    return cur

# ---------------- provably fair ----------------
def crash_point_for(server_seed: str, client_seed: str, nonce: int) -> Decimal:
    """HMAC_SHA256(key=server_seed, msg=f"{client_seed}:{nonce}"); first 52 bits."""
    h_hex = hmac.new(server_seed.encode(), f"{client_seed}:{nonce}".encode(), hashlib.sha256).hexdigest()
    h = int(h_hex[:13], 16)
    e = 2 ** 52
    if h % HOUSE_EDGE_MOD == 0:
        return D("1.00")
    return D(math.floor((100 * e - h) / (e - h))) / 100

def mult_at(t_ms: float) -> Decimal:
    return D(math.exp(GROWTH_K * max(t_ms, 0)))

def floor2(x: Decimal) -> Decimal:
    return x.quantize(Q2, rounding=ROUND_DOWN)

# ---------------- views ----------------
def round_view(r, with_time=True):
    done = r["state"] in ("crashed", "finished")
    started = r["started_at"]
    cur = 1.0
    if r["state"] == "running" and started:
        cur = float(floor2(mult_at(now_ms() - started)))
    elif done and r["crash_point"] is not None:
        cur = float(r["crash_point"])
    v = {
        "round_id": r["round_id"], "state": r["state"], "created_at": r["created_at"],
        "started_at": started, "crashed_at": r["crashed_at"], "crash_point": num(r["crash_point"]) if done else None,
        "current_multiplier": cur, "server_seed_hash": r["server_seed_hash"],
        "server_seed": r["server_seed"] if done else None,
        "client_seed": r["client_seed"], "nonce": r["nonce"],
    }
    if with_time:
        v["server_time"] = now_ms()
        v["betting_ends_at"] = r["betting_ends_at"]
    return v

def my_bet_view(b):
    return {"bet_id": b["bet_id"], "round_id": b["round_id"], "amount": num(b["amount"]),
            "currency": b["currency"], "status": b["status"], "cashout_multiplier": num(b["cashout_multiplier"]),
            "payout": num(b["payout"]), "auto_cashout": num(b["auto_cashout"])}

def player_view(b):  # public: no bet_id, no auto_cashout, internal user_id only
    return {"user_id": b["user_id"], "username": b["username"], "amount": num(b["amount"]),
            "currency": b["currency"], "cashout_multiplier": num(b["cashout_multiplier"]),
            "payout": num(b["payout"]), "status": b["status"]}

async def balances(cur, uid):
    cols = ", ".join(f"{BAL_COL[c]} AS {c}" for c in CURRENCIES)
    await q(cur, f"SELECT {cols} FROM {USERS_TABLE} WHERE {USERS_ID_COL}=%s", (uid,))
    r = await cur.fetchone()
    return {f"{c}_balance": num(r[c]) for c in CURRENCIES}

# ---------------- ws ----------------
async def broadcast(msg: dict):
    data = json.dumps(msg)
    dead = []
    for ws in list(clients):
        try:
            await asyncio.wait_for(ws.send_text(data), 2)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)

def _tk_key():
    return hmac.new(GAME_TOKEN.encode(), b"crash-ws-ticket-v1", hashlib.sha256).digest()

def make_ticket(tg_id: int, ttl=60) -> str:
    p = base64.urlsafe_b64encode(json.dumps({"tg": tg_id, "exp": int(time.time()) + ttl, "n": secrets.token_hex(4)}).encode()).decode()
    return p + "." + hmac.new(_tk_key(), p.encode(), hashlib.sha256).hexdigest()

def check_ticket(t) -> int | None:
    try:
        p, sig = t.split(".", 1)
        if not hmac.compare_digest(sig, hmac.new(_tk_key(), p.encode(), hashlib.sha256).hexdigest()):
            return None
        d = json.loads(base64.urlsafe_b64decode(p.encode()))
        return int(d["tg"]) if d["exp"] >= time.time() else None
    except Exception:
        return None

# ---------------- rate limit ----------------
_rl = defaultdict(deque)
def rate_limit(kind, key, limit, window):
    dq, t = _rl[(kind, key)], time.monotonic()
    while dq and dq[0] < t - window:
        dq.popleft()
    if len(dq) >= limit:
        raise ApiError(429, "RATE_LIMITED")
    dq.append(t)

# ---------------- helpers ----------------
async def current_round_row(cur):
    await q(cur, "SELECT * FROM crash_rounds ORDER BY created_at DESC LIMIT 1")
    return await cur.fetchone()

async def history(cur, n=20):
    await q(cur, "SELECT round_id,crash_point,created_at FROM crash_rounds WHERE state IN ('crashed','finished') ORDER BY created_at DESC LIMIT %s", (n,))
    return [{"round_id": r["round_id"], "crash_point": num(r["crash_point"]), "created_at": r["created_at"]} for r in await cur.fetchall()]

async def resolve_user(cur, body):
    t = body.get("tg_user") or {}
    try:
        tg = int(t["id"])
    except Exception:
        raise ApiError(401, "UNAUTHORIZED")
    await q(cur, f"SELECT {USERS_ID_COL} AS id FROM {USERS_TABLE} WHERE {USERS_TG_COL}=%s", (tg,))
    r = await cur.fetchone()
    if not r:
        raise ApiError(401, "UNAUTHORIZED")
    name = (t.get("username") or t.get("name") or f"user{r['id']}")[:64]
    return tg, r["id"], name

async def snapshot_for(uid):
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            r = await current_round_row(cur)
            my = None
            players = []
            if r:
                await q(cur, "SELECT * FROM crash_bets WHERE round_id=%s ORDER BY placed_at", (r["round_id"],))
                bets = await cur.fetchall()
                players = [player_view(b) for b in bets]
                mine = [b for b in bets if b["user_id"] == uid]
                my = my_bet_view(mine[0]) if mine else None
            hist = await history(cur)
            await c.commit()
    rv = round_view(r, with_time=False) if r else None
    return {"type": "snapshot", "server_time": now_ms(), "round": rv, "my_bet": my, "history": hist, "players": players}

async def apply_balance(cur, uid, currency, delta: Decimal, ttype: str, desc: str):
    """Change users.<col> inside the caller's transaction + write a row to core's `transactions` ledger.
    Raises INSUFFICIENT_BALANCE if the result would be negative. Lock order: round -> bet -> user."""
    col = BAL_COL[currency]
    await q(cur, f"SELECT {col} AS b FROM {USERS_TABLE} WHERE {USERS_ID_COL}=%s FOR UPDATE", (uid,))
    row = await cur.fetchone()
    if not row:
        raise ApiError(401, "UNAUTHORIZED")
    before = row["b"] or D(0)
    after = before + delta
    if after < 0:
        raise ApiError(402, "INSUFFICIENT_BALANCE")
    await q(cur, f"UPDATE {USERS_TABLE} SET {col}=%s, updated_at=UTC_TIMESTAMP() WHERE {USERS_ID_COL}=%s", (after, uid))
    await q(cur, """INSERT INTO transactions (user_id,amount,balance_before,balance_after,type,source,description,token,created_at)
                    VALUES (%s,%s,%s,%s,%s,'crash',%s,%s,UTC_TIMESTAMP())""",
            (uid, delta, before, after, ttype, desc[:255], currency.upper()))

# ---------------- settlement (call with round row locked) ----------------
async def settle(cur, bet, mult: Decimal, ts: int):
    payout = (bet["amount"] * mult).quantize(Q2, rounding=ROUND_DOWN)
    await q(cur, "UPDATE crash_bets SET status='cashed_out', cashout_multiplier=%s, payout=%s, cashed_out_at=%s WHERE bet_id=%s AND status='active'",
            (mult, payout, ts, bet["bet_id"]))
    if cur.rowcount != 1:
        raise ApiError(400, "BET_NOT_ACTIVE")
    await apply_balance(cur, bet["user_id"], bet["currency"], payout, "crash_win", bet["round_id"])
    return payout

def cashout_event(bet, mult, payout):
    p = player_view(bet)
    p.update(cashout_multiplier=float(mult), payout=float(payout), status="cashed_out")
    return {"type": "player_cashout", "round_id": bet["round_id"], "player": p}

# ---------------- engine ----------------
async def create_round():
    async with tx() as c, c.cursor() as cur:
        await q(cur, "SELECT COALESCE(MAX(nonce),0)+1 AS n FROM crash_rounds FOR UPDATE")
        nonce = (await cur.fetchone())["n"]
        seed = secrets.token_hex(32)
        t = now_ms()
        r = {"round_id": "r_" + uuid.uuid4().hex[:16], "state": "waiting", "created_at": t, "started_at": None,
             "crashed_at": None, "betting_ends_at": t + BETTING_MS, "crash_point": None, "server_seed": seed,
             "server_seed_hash": hashlib.sha256(seed.encode()).hexdigest(), "client_seed": secrets.token_hex(8), "nonce": nonce}
        await q(cur, """INSERT INTO crash_rounds (round_id,state,created_at,betting_ends_at,server_seed,server_seed_hash,client_seed,nonce)
                        VALUES (%s,'waiting',%s,%s,%s,%s,%s,%s)""",
                (r["round_id"], t, r["betting_ends_at"], seed, r["server_seed_hash"], r["client_seed"], nonce))
    log.info("[CRASH] round created")
    await broadcast({"type": "round_created", "round_id": r["round_id"], "created_at": t, "betting_ends_at": r["betting_ends_at"],
                     "server_time": now_ms(), "server_seed_hash": r["server_seed_hash"], "client_seed": r["client_seed"], "nonce": nonce})
    return r

async def start_round(r):
    async with round_lock:
        async with tx() as c, c.cursor() as cur:
            await q(cur, "SELECT * FROM crash_rounds WHERE round_id=%s FOR UPDATE", (r["round_id"],))
            r = await cur.fetchone()
            t = now_ms()
            await q(cur, "UPDATE crash_rounds SET state='running', started_at=%s WHERE round_id=%s", (t, r["round_id"]))
            await q(cur, "UPDATE crash_bets SET status='active' WHERE round_id=%s AND status='pending'", (r["round_id"],))
            r["state"], r["started_at"] = "running", t
    log.info("[CRASH] round started")
    await broadcast({"type": "round_started", "round_id": r["round_id"], "started_at": t, "server_time": t})
    return r

async def auto_cashouts(r, upper: Decimal, strict: bool):
    """Cash out active bets whose auto_cashout was passed. Caller holds round_lock."""
    events = []
    async with tx() as c, c.cursor() as cur:
        op = "<" if strict else "<="
        await q(cur, f"SELECT * FROM crash_bets WHERE round_id=%s AND status='active' AND auto_cashout IS NOT NULL AND auto_cashout {op} %s FOR UPDATE",
                (r["round_id"], upper))
        for b in await cur.fetchall():
            ts = now_ms()
            payout = await settle(cur, b, b["auto_cashout"], ts)
            events.append(cashout_event(b, b["auto_cashout"], payout))
    for e in events:
        log.info("[CRASH] cashout accepted (auto)")
        await broadcast(e)

async def run_running(r):
    cp = crash_point_for(r["server_seed"], r["client_seed"], r["nonce"])
    t_crash_ms = math.log(float(cp)) / GROWTH_K if cp > 1 else 0
    tick = 0
    while True:
        t = now_ms() - r["started_at"]
        if t >= t_crash_ms:
            break
        m = mult_at(t)
        if tick % 2 == 0:
            await broadcast({"type": "multiplier_update", "round_id": r["round_id"], "multiplier": float(floor2(m)), "server_time": now_ms()})
        async with round_lock:
            await auto_cashouts(r, m, strict=False)
        tick += 1
        await asyncio.sleep(max(0.005, min(0.1, (t_crash_ms - t) / 1000)))
    # crash: atomic wrt cashout
    async with round_lock:
        await auto_cashouts(r, cp, strict=True)
        async with tx() as c, c.cursor() as cur:
            await q(cur, "SELECT * FROM crash_rounds WHERE round_id=%s FOR UPDATE", (r["round_id"],))
            t = now_ms()
            await q(cur, "UPDATE crash_bets SET status='lost' WHERE round_id=%s AND status IN ('active','pending')", (r["round_id"],))
            await q(cur, "UPDATE crash_rounds SET state='crashed', crashed_at=%s, crash_point=%s WHERE round_id=%s", (t, cp, r["round_id"]))
    log.info("[CRASH] round crashed")
    await broadcast({"type": "round_crashed", "round_id": r["round_id"], "crash_point": float(cp), "crashed_at": t,
                     "server_time": t, "server_seed": r["server_seed"]})
    r["state"], r["crashed_at"] = "crashed", t
    return r

async def engine():
    while True:
        try:
            async with pool.acquire() as c:
                async with c.cursor() as cur:
                    await q(cur, "SELECT * FROM crash_rounds WHERE state IN ('waiting','running','crashed') ORDER BY created_at DESC LIMIT 1")
                    r = await cur.fetchone()
                    await c.commit()
            if not r:
                r = await create_round()   # (resume after restart is deterministic: seed + started_at are persisted)
            if r["state"] == "waiting":
                await asyncio.sleep(max(0, (r["betting_ends_at"] - now_ms()) / 1000))
                r = await start_round(r)
            if r["state"] == "running":
                r = await run_running(r)
            if r["state"] == "crashed":
                await asyncio.sleep(max(0, (r["crashed_at"] + CRASH_MS - now_ms()) / 1000))
                async with tx() as c, c.cursor() as cur:
                    await q(cur, "UPDATE crash_rounds SET state='finished' WHERE round_id=%s", (r["round_id"],))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[CRASH] engine error")
            await asyncio.sleep(2)

# ---------------- app ----------------
@asynccontextmanager
async def lifespan(app):
    global pool
    if not GAME_TOKEN:
        raise RuntimeError("GAME_UNDERBOARD_TOKEN is not set")
    pool = await aiomysql.create_pool(
        host=os.environ["DB_HOST"], port=int(os.getenv("DB_PORT", "3306")), user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], db=os.environ["DB_NAME"], autocommit=False,
        minsize=1, maxsize=10, cursorclass=aiomysql.DictCursor, charset="utf8mb4")
    async with tx() as c, c.cursor() as cur:
        for ddl in SCHEMA:
            await cur.execute(ddl)
    task = asyncio.create_task(engine())
    yield
    task.cancel()
    pool.close(); await pool.wait_closed()

app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

@app.exception_handler(ApiError)
async def api_err(_, e: ApiError):
    return JSONResponse({"success": False, "error": e.code}, status_code=e.status)

@app.middleware("http")
async def guard(request: Request, call_next):
    if request.url.path.startswith("/crash/"):
        tok = request.headers.get("x-game-token", "")
        if not hmac.compare_digest(tok.encode(), GAME_TOKEN.encode()):
            log.info("[CRASH] invalid game token")
            return JSONResponse({"success": False, "error": "INVALID_GAME_TOKEN"}, status_code=401)
    try:
        return await call_next(request)
    except ApiError as e:
        return JSONResponse({"success": False, "error": e.code}, status_code=e.status)
    except Exception:
        log.exception("[CRASH] unhandled")
        return JSONResponse({"success": False, "error": "INTERNAL_ERROR"}, status_code=500)

@app.get("/health")
async def health():
    log.info("[CRASH] health check")
    return {"success": True, "service": "crash", "status": "ok"}

async def jbody(request):
    try:
        b = await request.json()
        return b if isinstance(b, dict) else {}
    except Exception:
        return {}

@app.post("/crash/config")
async def r_config(request: Request):
    return {"success": True, "min_bet": int(MIN_BET), "max_bet": int(MAX_BET), "currencies": list(CURRENCIES),
            "betting_duration_ms": BETTING_MS, "crash_duration_ms": CRASH_MS}

@app.post("/crash/ws-ticket")
async def r_ticket(request: Request):
    b = await jbody(request)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            tg, _, _ = await resolve_user(cur, b)
    return {"success": True, "ticket": make_ticket(tg), "ws_url": PUBLIC_WS_URL, "expires_in": 60}

@app.post("/crash/current-round")
async def r_current(request: Request):
    b = await jbody(request)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            _, uid, _ = await resolve_user(cur, b)
            await c.commit()
    s = await snapshot_for(uid)
    rv = s["round"]
    if rv:
        rv["server_time"] = s["server_time"]
        rv["betting_ends_at"] = (await _betting_ends(rv["round_id"]))
    return {"success": True, "round": rv, "my_bet": s["my_bet"], "history": s["history"], "players": s["players"]}

async def _betting_ends(rid):
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await q(cur, "SELECT betting_ends_at FROM crash_rounds WHERE round_id=%s", (rid,))
            r = await cur.fetchone(); await c.commit()
            return r["betting_ends_at"]

@app.post("/crash/place-bet")
async def r_bet(request: Request):
    b = await jbody(request)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            tg, uid, name = await resolve_user(cur, b)
            await c.commit()
    rate_limit("bet", tg, 5, 10)
    cur_ = b.get("currency")
    if cur_ not in CURRENCIES:
        raise ApiError(400, "INVALID_AMOUNT")
    try:
        amount = D(str(b.get("amount")))
        if not amount.is_finite() or amount != amount.quantize(Q2) or not (MIN_BET <= amount <= MAX_BET):
            raise ValueError
    except (InvalidOperation, ValueError):
        raise ApiError(400, "INVALID_AMOUNT")
    auto = b.get("auto_cashout")
    if auto is not None:
        try:
            auto = D(str(auto)).quantize(Q2, rounding=ROUND_DOWN)
            if auto < D("1.01"):
                raise ValueError
        except (InvalidOperation, ValueError):
            raise ApiError(400, "INVALID_AMOUNT")
    key = str(b.get("idempotency_key") or "")
    rid = str(b.get("round_id") or "")
    if not (8 <= len(key) <= 64) or not rid:
        raise ApiError(400, "INVALID_AMOUNT")

    def bet_resp(bet, bal):
        v = my_bet_view(bet); v["placed_at"] = bet["placed_at"]
        return {"success": True, "bet": v, **bal}

    async def existing():
        async with pool.acquire() as c:
            async with c.cursor() as cur:
                await q(cur, "SELECT * FROM crash_bets WHERE user_id=%s AND idempotency_key=%s", (uid, key))
                bet = await cur.fetchone()
                bal = await balances(cur, uid) if bet else None
                await c.commit()
                return (bet, bal)

    bet, bal = await existing()
    if bet:
        return bet_resp(bet, bal)
    try:
        async with tx() as c, c.cursor() as cur:
            await q(cur, "SELECT * FROM crash_rounds WHERE round_id=%s FOR UPDATE", (rid,))
            r = await cur.fetchone()
            if not r:
                raise ApiError(409, "ROUND_NOT_FOUND")
            if r["state"] != "waiting" or now_ms() >= r["betting_ends_at"]:
                raise ApiError(400, "BETTING_CLOSED")
            await q(cur, "SELECT 1 FROM crash_bets WHERE round_id=%s AND user_id=%s", (rid, uid))
            if await cur.fetchone():
                raise ApiError(400, "DUPLICATE_BET")
            await apply_balance(cur, uid, cur_, -amount, "crash_bet", rid)
            bid, t = "b_" + uuid.uuid4().hex[:16], now_ms()
            await q(cur, """INSERT INTO crash_bets (bet_id,round_id,user_id,username,amount,currency,status,auto_cashout,placed_at,idempotency_key)
                            VALUES (%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s)""", (bid, rid, uid, name, amount, cur_, auto, t, key))
            await q(cur, "SELECT * FROM crash_bets WHERE bet_id=%s", (bid,))
            bet = await cur.fetchone()
            bal = await balances(cur, uid)
    except aiomysql.IntegrityError:   # concurrent duplicate of same idempotency_key
        bet, bal = await existing()
        if bet:
            return bet_resp(bet, bal)
        raise ApiError(400, "DUPLICATE_BET")
    log.info("[CRASH] bet accepted")
    await broadcast({"type": "player_bet", "round_id": rid, "player": player_view(bet)})
    return bet_resp(bet, bal)

@app.post("/crash/cashout")
async def r_cashout(request: Request):
    b = await jbody(request)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            tg, uid, _ = await resolve_user(cur, b)
            await c.commit()
    rate_limit("cashout", tg, 10, 10)
    bid, key = str(b.get("bet_id") or ""), str(b.get("idempotency_key") or "")
    if not (8 <= len(key) <= 64):
        raise ApiError(400, "BET_NOT_ACTIVE")

    def resp(bet, bal):
        return {"success": True, "cashout": {"bet_id": bet["bet_id"], "round_id": bet["round_id"], "multiplier": num(bet["cashout_multiplier"]),
                "payout": num(bet["payout"]), "currency": bet["currency"], "cashed_out_at": bet["cashed_out_at"]}, **bal}

    event = None
    async with round_lock:   # same lock the engine holds while crashing => no cashout/crash interleaving
        async with tx() as c, c.cursor() as cur:
            await q(cur, "SELECT round_id FROM crash_bets WHERE bet_id=%s AND user_id=%s", (bid, uid))
            x = await cur.fetchone()
            if not x:
                raise ApiError(404, "BET_NOT_FOUND")
            await q(cur, "SELECT * FROM crash_rounds WHERE round_id=%s FOR UPDATE", (x["round_id"],))
            r = await cur.fetchone()
            await q(cur, "SELECT * FROM crash_bets WHERE bet_id=%s FOR UPDATE", (bid,))
            bet = await cur.fetchone()
            if bet["status"] == "cashed_out":
                if bet["cashout_key"] == key:
                    return resp(bet, await balances(cur, uid))
                raise ApiError(409, "ALREADY_CASHED_OUT")
            if bet["status"] == "lost":
                raise ApiError(408, "CASHOUT_TOO_LATE")
            if bet["status"] != "active":
                raise ApiError(400, "BET_NOT_ACTIVE")
            if r["state"] in ("crashed", "finished"):
                raise ApiError(408, "CASHOUT_TOO_LATE")
            if r["state"] != "running":
                raise ApiError(400, "ROUND_NOT_RUNNING")
            cp = crash_point_for(r["server_seed"], r["client_seed"], r["nonce"])
            m = mult_at(now_ms() - r["started_at"])
            if m >= cp:
                raise ApiError(408, "CASHOUT_TOO_LATE")
            mult = max(floor2(m), D("1.00"))
            ts = now_ms()
            payout = await settle(cur, bet, mult, ts)
            await q(cur, "UPDATE crash_bets SET cashout_key=%s WHERE bet_id=%s", (key, bid))
            await q(cur, "SELECT * FROM crash_bets WHERE bet_id=%s", (bid,))
            bet = await cur.fetchone()
            bal = await balances(cur, uid)
            event = cashout_event(bet, mult, payout)
    log.info("[CRASH] cashout accepted")
    await broadcast(event)
    return resp(bet, bal)

@app.post("/crash/my-bets")
async def r_my_bets(request: Request):
    b = await jbody(request)
    n = min(max(int(b.get("limit") or 20), 1), 100)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            _, uid, _ = await resolve_user(cur, b)
            await q(cur, """SELECT b.*, r.crash_point FROM crash_bets b JOIN crash_rounds r USING(round_id)
                            WHERE b.user_id=%s AND r.state IN ('crashed','finished') ORDER BY b.placed_at DESC LIMIT %s""", (uid, n))
            rows = await cur.fetchall(); await c.commit()
    return {"success": True, "bets": [{**my_bet_view(x), "placed_at": x["placed_at"], "cashed_out_at": x["cashed_out_at"],
                                       "crash_point": num(x["crash_point"])} for x in rows]}

@app.post("/crash/round-history")
async def r_history(request: Request):
    b = await jbody(request)
    n = min(max(int(b.get("limit") or 20), 1), 100)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await resolve_user(cur, b)
            h = await history(cur, n); await c.commit()
    return {"success": True, "rounds": h}

@app.post("/crash/round-details")
async def r_details(request: Request):
    b = await jbody(request)
    async with pool.acquire() as c:
        async with c.cursor() as cur:
            await resolve_user(cur, b)
            await q(cur, "SELECT * FROM crash_rounds WHERE round_id=%s", (str(b.get("round_id") or ""),))
            r = await cur.fetchone(); await c.commit()
    if not r:
        raise ApiError(404, "ROUND_NOT_FOUND")
    if r["state"] not in ("crashed", "finished"):
        raise ApiError(403, "ROUND_NOT_FINISHED")
    return {"success": True, "round": {"round_id": r["round_id"], "crash_point": num(r["crash_point"]),
            "server_seed_hash": r["server_seed_hash"], "server_seed": r["server_seed"], "client_seed": r["client_seed"],
            "nonce": r["nonce"], "created_at": r["created_at"], "crashed_at": r["crashed_at"]}}

@app.websocket("/ws/crash")
async def ws_crash(ws: WebSocket):
    await ws.accept()
    uid = None
    try:
        msg = json.loads(await asyncio.wait_for(ws.receive_text(), 10))
        tg = check_ticket(msg.get("ticket", "")) if msg.get("type") == "auth" else None
        if tg:
            async with pool.acquire() as c:
                async with c.cursor() as cur:
                    await q(cur, f"SELECT {USERS_ID_COL} AS id FROM {USERS_TABLE} WHERE {USERS_TG_COL}=%s", (tg,))
                    u = await cur.fetchone(); await c.commit()
            uid = u["id"] if u else None
        if not uid:
            await ws.send_text(json.dumps({"type": "auth_error", "error": "UNAUTHORIZED"}))
            await ws.close(code=4001)
            return
        clients[ws] = uid
        log.info("[CRASH] websocket connected")
        await ws.send_text(json.dumps(await snapshot_for(uid)))
        while True:
            await ws.receive_text()   # client messages ignored (keeps connection open)
    except (WebSocketDisconnect, asyncio.TimeoutError, json.JSONDecodeError):
        pass
    except Exception:
        log.exception("[CRASH] ws error")
    finally:
        if clients.pop(ws, None) is not None:
            log.info("[CRASH] websocket disconnected")
