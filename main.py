"""ZERO7 Crash v2 — ALL game logic here (Render). NO direct DB access:
every read/write of money and history goes through the PHP internal API (core.php?action=crash_int_*)."""
import asyncio, base64, hashlib, hmac, json, logging, math, os, secrets, time, uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation, ROUND_DOWN

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("crash")

GAME_TOKEN = os.getenv("GAME_UNDERBOARD_TOKEN", "")
PHP_API_URL = os.getenv("PHP_API_URL", "")
PUBLIC_WS_URL = os.getenv("PUBLIC_WS_URL", "")
BETTING_MS = int(os.getenv("CRASH_BETTING_MS", "5000"))
CRASH_MS = int(os.getenv("CRASH_CRASH_MS", "2500"))
D = Decimal
MIN_BET, MAX_BET = D(os.getenv("CRASH_MIN_BET", "1")), D(os.getenv("CRASH_MAX_BET", "100000"))
GROWTH_K = float(os.getenv("CRASH_GROWTH_K", "0.00006"))   # m(t)=exp(K*t_ms)
HOUSE_EDGE_MOD = 33   # measured RTP ~96.4%
CURRENCIES = ("xbite", "gram", "stars")
Q2 = D("0.01")
now_ms = lambda: int(time.time() * 1000)
num = lambda x: None if x is None else float(x)

class ApiError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code

class Unavailable(Exception):
    """PHP/DB unreachable or misbehaving."""

# ---------------- PHP internal API client ----------------
http: httpx.AsyncClient = None

async def php(action, payload, timeout=6.0):
    try:
        r = await http.post(PHP_API_URL, params={"action": action}, json=payload,
                            headers={"X-Game-Token": GAME_TOKEN}, timeout=timeout)
    except httpx.HTTPError as e:
        log.warning("[CRASH] php transport error: %s", type(e).__name__)
        raise Unavailable()
    try:
        j = r.json()
    except ValueError:
        log.warning("[CRASH] php non-JSON response status=%s", r.status_code)
        raise Unavailable()
    if not isinstance(j, dict):
        raise Unavailable()
    if j.get("success") is True:
        return j
    code = str(j.get("error", "INTERNAL_ERROR"))
    if r.status_code >= 500 or code == "INVALID_GAME_TOKEN":
        log.warning("[CRASH] php error status=%s code=%s", r.status_code, code)
        raise Unavailable()
    raise ApiError(r.status_code, code)

async def persist(action, payload):
    """Retry until PHP answers (fail-closed: the engine waits instead of running without a record)."""
    delay = 1
    while True:
        try:
            return await php(action, payload, 8)
        except Unavailable:
            log.warning("[CRASH] persist %s failed, retry in %ss", action, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)

# ---------------- provably fair ----------------
def crash_point_for(server_seed, client_seed, nonce) -> Decimal:
    """HMAC_SHA256(key=server_seed, msg=f"{client_seed}:{nonce}"); first 52 bits."""
    h = int(hmac.new(server_seed.encode(), f"{client_seed}:{nonce}".encode(), hashlib.sha256).hexdigest()[:13], 16)
    e = 2 ** 52
    if h % HOUSE_EDGE_MOD == 0:
        return D("1.00")
    return D(math.floor((100 * e - h) / (e - h))) / 100

mult_at = lambda t_ms: D(math.exp(GROWTH_K * max(t_ms, 0)))
floor2 = lambda x: x.quantize(Q2, rounding=ROUND_DOWN)

# ---------------- state (authoritative, in memory) ----------------
class State:
    def __init__(self):
        self.round = None
        self.bets = {}          # bet_id -> bet (current round)
        self.by_user = {}       # user_id -> bet
        self.reserved = set()   # users with a bet request in flight
        self.inflight = 0
        self.history = deque(maxlen=20)   # newest first
        self.last_nonce = 0
        self.tasks = set()
        self.ready = False
st = State()
lock = asyncio.Lock()
clients: dict = {}

def spawn(coro):
    t = asyncio.create_task(coro)
    st.tasks.add(t)
    t.add_done_callback(st.tasks.discard)
    return t

def bet_from_row(r):
    n = lambda k: None if r.get(k) is None else D(str(r[k]))
    return {"bet_id": r["bet_id"], "round_id": r["round_id"], "user_id": int(r["user_id"]), "username": r.get("username") or "",
            "amount": D(str(r["amount"])), "currency": r["currency"], "status": r["status"],
            "auto_cashout": n("auto_cashout"), "cashout_multiplier": n("cashout_multiplier"), "payout": n("payout"),
            "placed_at": int(r["placed_at"]), "cashed_out_at": None if r.get("cashed_out_at") is None else int(r["cashed_out_at"]),
            "cashout_key": r.get("cashout_key"), "idem": r.get("idempotency_key"), "settling": False}

def round_from_row(r):
    o = lambda k: None if r.get(k) is None else int(r[k])
    rd = {"round_id": r["round_id"], "state": r["state"], "created_at": int(r["created_at"]), "started_at": o("started_at"),
          "crashed_at": o("crashed_at"), "betting_ends_at": int(r["betting_ends_at"]),
          "crash_point": None if r.get("crash_point") is None else D(str(r["crash_point"])),
          "server_seed": r["server_seed"], "server_seed_hash": r["server_seed_hash"], "client_seed": r["client_seed"], "nonce": int(r["nonce"])}
    rd["cp"] = crash_point_for(rd["server_seed"], rd["client_seed"], rd["nonce"])
    return rd

def round_payload(r, state=None):
    return {"round_id": r["round_id"], "state": state or r["state"], "created_at": r["created_at"], "started_at": r["started_at"],
            "crashed_at": r["crashed_at"], "betting_ends_at": r["betting_ends_at"],
            "crash_point": None if r["crash_point"] is None else str(r["crash_point"]),
            "server_seed": r["server_seed"], "server_seed_hash": r["server_seed_hash"], "client_seed": r["client_seed"], "nonce": r["nonce"]}

# ---------------- views ----------------
def round_view(r, with_time=True):
    done = r["state"] in ("crashed", "finished")
    cur = 1.0
    if r["state"] == "running":
        cur = float(floor2(mult_at(now_ms() - r["started_at"])))
    elif done:
        cur = float(r["crash_point"])
    v = {"round_id": r["round_id"], "state": "waiting" if r["state"] == "closing" else r["state"], "created_at": r["created_at"],
         "started_at": r["started_at"], "crashed_at": r["crashed_at"], "crash_point": num(r["crash_point"]) if done else None,
         "current_multiplier": cur, "server_seed_hash": r["server_seed_hash"], "server_seed": r["server_seed"] if done else None,
         "client_seed": r["client_seed"], "nonce": r["nonce"]}
    if with_time:
        v["server_time"], v["betting_ends_at"] = now_ms(), r["betting_ends_at"]
    return v

def my_bet_view(b):
    return {"bet_id": b["bet_id"], "round_id": b["round_id"], "amount": num(b["amount"]), "currency": b["currency"],
            "status": b["status"], "cashout_multiplier": num(b["cashout_multiplier"]), "payout": num(b["payout"]),
            "auto_cashout": num(b["auto_cashout"])}

def player_view(b):   # public: no bet_id, no auto_cashout
    return {"user_id": b["user_id"], "username": b["username"], "amount": num(b["amount"]), "currency": b["currency"],
            "cashout_multiplier": num(b["cashout_multiplier"]), "payout": num(b["payout"]), "status": b["status"]}

def cashout_event(b):
    return {"type": "player_cashout", "round_id": b["round_id"], "player": player_view(b)}

def snapshot_for(uid):
    r = st.round
    return {"type": "snapshot", "server_time": now_ms(), "round": round_view(r, False),
            "my_bet": my_bet_view(st.by_user[uid]) if uid in st.by_user else None,
            "history": list(st.history), "players": [player_view(b) for b in st.bets.values()]}

# ---------------- ws ----------------
async def broadcast(msg):
    data = json.dumps(msg)
    dead = []
    for ws in list(clients):
        try:
            await asyncio.wait_for(ws.send_text(data), 2)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)

_tk_key = lambda: hmac.new(GAME_TOKEN.encode(), b"crash-ws-ticket-v1", hashlib.sha256).digest()

def make_ticket(tg, uid, name, ttl=60):
    p = base64.urlsafe_b64encode(json.dumps({"tg": tg, "uid": uid, "name": name, "exp": int(time.time()) + ttl, "n": secrets.token_hex(4)}).encode()).decode()
    return p + "." + hmac.new(_tk_key(), p.encode(), hashlib.sha256).hexdigest()

def check_ticket(t):
    try:
        p, sig = t.split(".", 1)
        if not hmac.compare_digest(sig, hmac.new(_tk_key(), p.encode(), hashlib.sha256).hexdigest()):
            return None
        d = json.loads(base64.urlsafe_b64decode(p.encode()))
        return d if d["exp"] >= time.time() else None
    except Exception:
        return None

_rl = defaultdict(deque)
def rate_limit(kind, key, limit, window):
    dq, t = _rl[(kind, key)], time.monotonic()
    while dq and dq[0] < t - window:
        dq.popleft()
    if len(dq) >= limit:
        raise ApiError(429, "RATE_LIMITED")
    dq.append(t)

# ---------------- settlement ----------------
async def settle_persist(bet, mult, key, ts):
    """Idempotent on the PHP side (by bet_id + cashout_key). Retries until PHP answers."""
    res = await persist("crash_int_bet_settle", {"bet_id": bet["bet_id"], "user_id": bet["user_id"], "multiplier": str(mult),
                                                 "cashout_key": key, "cashed_out_at": ts})
    b = res["bet"]
    bet.update(status="cashed_out", cashout_multiplier=D(str(b["cashout_multiplier"])), payout=D(str(b["payout"])),
               cashed_out_at=int(b["cashed_out_at"]), cashout_key=key)
    return res

async def bg_settle(bet, mult, key, ts):
    try:
        await settle_persist(bet, mult, key, ts)
    except Exception:
        log.exception("[CRASH] background settle failed bet=%s", bet["bet_id"])

def decide_auto(r, limit, strict):
    """Caller holds lock. Mark bets whose auto-cashout was passed as cashed out (final decision in memory)."""
    out = []
    for b in st.bets.values():
        a = b["auto_cashout"]
        if b["status"] == "active" and not b["settling"] and a is not None and (a < limit if strict else a <= limit):
            b["settling"] = True
            ts = now_ms()
            b.update(status="cashed_out", cashout_multiplier=a, payout=(b["amount"] * a).quantize(Q2, rounding=ROUND_DOWN), cashed_out_at=ts)
            out.append((b, a, "auto-" + b["bet_id"], ts))
    return out

async def flush_auto(items):
    for b, a, key, ts in items:
        log.info("[CRASH] cashout accepted (auto)")
        await broadcast(cashout_event(b))
        spawn(bg_settle(b, a, key, ts))

# ---------------- engine ----------------
async def create_round():
    nonce = st.last_nonce + 1
    seed = secrets.token_hex(32)
    t = now_ms()
    r = {"round_id": "r_" + uuid.uuid4().hex[:16], "state": "waiting", "created_at": t, "started_at": None, "crashed_at": None,
         "betting_ends_at": t + BETTING_MS, "crash_point": None, "server_seed": seed,
         "server_seed_hash": hashlib.sha256(seed.encode()).hexdigest(), "client_seed": secrets.token_hex(8), "nonce": nonce}
    r["cp"] = crash_point_for(seed, r["client_seed"], nonce)
    await persist("crash_int_round_save", round_payload(r))      # persisted BEFORE bets are accepted
    st.last_nonce = nonce
    async with lock:
        st.round, st.bets, st.by_user, st.reserved = r, {}, {}, set()
    log.info("[CRASH] round created")
    await broadcast({"type": "round_created", "round_id": r["round_id"], "created_at": t, "betting_ends_at": r["betting_ends_at"],
                     "server_time": now_ms(), "server_seed_hash": r["server_seed_hash"], "client_seed": r["client_seed"], "nonce": nonce})
    return r

async def start_round(r):
    async with lock:
        r["state"] = "closing"          # new bets rejected from here
    deadline = time.monotonic() + 6
    while st.inflight > 0 and time.monotonic() < deadline:   # let in-flight bet requests finish
        await asyncio.sleep(0.02)
    t = now_ms()
    r["started_at"] = t
    res = await persist("crash_int_round_save", round_payload(r, "running"))   # PHP activates pending bets, returns authoritative list
    async with lock:
        bets = [bet_from_row(x) for x in res.get("bets", [])]
        st.bets = {b["bet_id"]: b for b in bets}
        st.by_user = {b["user_id"]: b for b in bets}
        r["state"] = "running"
    log.info("[CRASH] round started")
    await broadcast({"type": "round_started", "round_id": r["round_id"], "started_at": t, "server_time": now_ms()})

async def run_running(r):
    cp = r["cp"]
    t_crash = math.log(float(cp)) / GROWTH_K if cp > 1 else 0
    tick = 0
    while True:
        t = now_ms() - r["started_at"]
        if t >= t_crash:
            break
        m = mult_at(t)
        if tick % 2 == 0:
            await broadcast({"type": "multiplier_update", "round_id": r["round_id"], "multiplier": float(floor2(m)), "server_time": now_ms()})
        async with lock:
            items = decide_auto(r, m, strict=False)
        await flush_auto(items)
        tick += 1
        await asyncio.sleep(max(0.005, min(0.1, (t_crash - t) / 1000)))
    async with lock:   # atomic wrt manual cashout decisions
        items = decide_auto(r, cp, strict=True)
        for b in st.bets.values():
            if b["status"] in ("active", "pending") and not b["settling"]:
                b["status"] = "lost"
        t = now_ms()
        r.update(state="crashed", crashed_at=t, crash_point=cp)
    await flush_auto(items)
    log.info("[CRASH] round crashed")
    await broadcast({"type": "round_crashed", "round_id": r["round_id"], "crash_point": float(cp), "crashed_at": t,
                     "server_time": t, "server_seed": r["server_seed"]})
    st.history.appendleft({"round_id": r["round_id"], "crash_point": float(cp), "created_at": r["created_at"]})
    await asyncio.gather(*list(st.tasks), return_exceptions=True)          # all payouts persisted first
    await persist("crash_int_round_save", round_payload(r, "crashed"))      # PHP marks remaining bets lost

async def finish_round(r):
    await asyncio.sleep(max(0, (r["crashed_at"] + CRASH_MS - now_ms()) / 1000))
    await persist("crash_int_round_save", round_payload(r, "finished"))
    r["state"] = "finished"

async def load_state():
    j = await persist("crash_int_load", {})
    st.last_nonce = int(j.get("last_nonce") or 0)
    st.history = deque(({"round_id": x["round_id"], "crash_point": float(x["crash_point"]), "created_at": int(x["created_at"])}
                        for x in j.get("history", [])), maxlen=20)
    if j.get("round"):
        r = round_from_row(j["round"])
        bets = [bet_from_row(x) for x in j.get("bets", [])]
        st.round, st.bets, st.by_user = r, {b["bet_id"]: b for b in bets}, {b["user_id"]: b for b in bets}
        log.info("[CRASH] resumed round state=%s", r["state"])

async def engine():
    await load_state()
    st.ready = True
    while True:
        try:
            r = st.round
            if r is None or r["state"] == "finished":
                r = await create_round()
            if r["state"] == "waiting":
                await asyncio.sleep(max(0, (r["betting_ends_at"] - now_ms()) / 1000))
            if r["state"] in ("waiting", "closing"):
                await start_round(r)
            if r["state"] == "running":
                await run_running(r)
            if r["state"] == "crashed":
                await finish_round(r)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[CRASH] engine error")
            await asyncio.sleep(2)

# ---------------- app ----------------
@asynccontextmanager
async def lifespan(app):
    global http
    if not GAME_TOKEN or not PHP_API_URL:
        raise RuntimeError("GAME_UNDERBOARD_TOKEN and PHP_API_URL must be set")
    http = httpx.AsyncClient()
    task = asyncio.create_task(engine())
    yield
    task.cancel()
    await http.aclose()

app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

@app.exception_handler(ApiError)
async def api_err(_, e):
    return JSONResponse({"success": False, "error": e.code}, status_code=e.status)

@app.middleware("http")
async def guard(request: Request, call_next):
    if request.url.path.startswith("/crash/"):
        if not hmac.compare_digest(request.headers.get("x-game-token", "").encode(), GAME_TOKEN.encode()):
            log.info("[CRASH] invalid game token")
            return JSONResponse({"success": False, "error": "INVALID_GAME_TOKEN"}, status_code=401)
    try:
        return await call_next(request)
    except ApiError as e:
        return JSONResponse({"success": False, "error": e.code}, status_code=e.status)
    except Unavailable:
        return JSONResponse({"success": False, "error": "CRASH_BACKEND_UNAVAILABLE"}, status_code=503)
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

def who(body):
    t = body.get("tg_user") or {}
    try:
        uid = int(t["user_id"])
        return int(t["id"]), uid, str(t.get("username") or t.get("name") or f"user{uid}")[:64]
    except Exception:
        raise ApiError(401, "UNAUTHORIZED")

def need_ready():
    if not st.ready or st.round is None:
        raise ApiError(503, "CRASH_BACKEND_UNAVAILABLE")

@app.post("/crash/config")
async def r_config(request: Request):
    return {"success": True, "min_bet": int(MIN_BET), "max_bet": int(MAX_BET), "currencies": list(CURRENCIES),
            "betting_duration_ms": BETTING_MS, "crash_duration_ms": CRASH_MS}

@app.post("/crash/ws-ticket")
async def r_ticket(request: Request):
    tg, uid, name = who(await jbody(request))
    return {"success": True, "ticket": make_ticket(tg, uid, name), "ws_url": PUBLIC_WS_URL, "expires_in": 60}

@app.post("/crash/current-round")
async def r_current(request: Request):
    _, uid, _ = who(await jbody(request))
    need_ready()
    s = snapshot_for(uid)
    rv = round_view(st.round)
    return {"success": True, "round": rv, "my_bet": s["my_bet"], "history": s["history"], "players": s["players"]}

@app.post("/crash/place-bet")
async def r_bet(request: Request):
    b = await jbody(request)
    tg, uid, name = who(b)
    need_ready()
    rate_limit("bet", tg, 5, 10)
    cur = b.get("currency")
    if cur not in CURRENCIES:
        raise ApiError(400, "INVALID_AMOUNT")
    try:
        amount = D(str(b.get("amount")))
        if not amount.is_finite() or amount != amount.quantize(Q2) or not (MIN_BET <= amount <= MAX_BET):
            raise ValueError
        auto = b.get("auto_cashout")
        if auto is not None:
            auto = D(str(auto)).quantize(Q2, rounding=ROUND_DOWN)
            if auto < D("1.01"):
                raise ValueError
    except (InvalidOperation, ValueError):
        raise ApiError(400, "INVALID_AMOUNT")
    key, rid = str(b.get("idempotency_key") or ""), str(b.get("round_id") or "")
    if not (8 <= len(key) <= 64) or not rid:
        raise ApiError(400, "INVALID_AMOUNT")
    payload = {"user_id": uid, "username": name, "round_id": rid, "currency": cur, "amount": str(amount),
               "auto_cashout": None if auto is None else str(auto), "idempotency_key": key}

    existing = st.by_user.get(uid)
    if existing and existing["idem"] == key:        # replay -> PHP is idempotent, returns current balances
        res = await php("crash_int_bet_place", {**payload, "bet_id": existing["bet_id"], "placed_at": existing["placed_at"]}, 4)
        bet = bet_from_row(res["bet"])
        return {"success": True, "bet": {**my_bet_view(bet), "placed_at": bet["placed_at"]}, **res["balances"]}

    async with lock:
        r = st.round
        if r["round_id"] != rid:
            raise ApiError(409, "ROUND_NOT_FOUND")
        if r["state"] != "waiting" or now_ms() >= r["betting_ends_at"]:
            raise ApiError(400, "BETTING_CLOSED")
        if uid in st.by_user or uid in st.reserved:
            raise ApiError(400, "DUPLICATE_BET")
        st.reserved.add(uid)
        st.inflight += 1
    try:
        res = await php("crash_int_bet_place", {**payload, "bet_id": "b_" + uuid.uuid4().hex[:16], "placed_at": now_ms()}, 4)
        bet = bet_from_row(res["bet"])
        async with lock:
            if st.round["round_id"] == rid:
                st.bets[bet["bet_id"]] = bet
                st.by_user[uid] = bet
    finally:
        st.reserved.discard(uid)
        st.inflight -= 1
    log.info("[CRASH] bet accepted")
    await broadcast({"type": "player_bet", "round_id": rid, "player": player_view(bet)})
    return {"success": True, "bet": {**my_bet_view(bet), "placed_at": bet["placed_at"]}, **res["balances"]}

@app.post("/crash/cashout")
async def r_cashout(request: Request):
    b = await jbody(request)
    tg, uid, _ = who(b)
    need_ready()
    rate_limit("cashout", tg, 10, 10)
    bid, key = str(b.get("bet_id") or ""), str(b.get("idempotency_key") or "")
    if not (8 <= len(key) <= 64) or not bid:
        raise ApiError(400, "BET_NOT_ACTIVE")

    def resp(bet, res):
        return {"success": True, "cashout": {"bet_id": bet["bet_id"], "round_id": bet["round_id"], "multiplier": num(bet["cashout_multiplier"]),
                "payout": num(bet["payout"]), "currency": bet["currency"], "cashed_out_at": bet["cashed_out_at"]}, **res["balances"]}

    replay = False
    async with lock:
        bet = st.bets.get(bid)
        if bet is not None and bet["user_id"] != uid:
            raise ApiError(404, "BET_NOT_FOUND")
        r = st.round
        if bet is None or bet["status"] == "cashed_out":
            replay = True                          # old round or already cashed: PHP answers idempotently
        else:
            if bet["status"] == "lost":
                raise ApiError(408, "CASHOUT_TOO_LATE")
            if bet["status"] != "active" or bet["settling"]:
                raise ApiError(400, "BET_NOT_ACTIVE")
            if r["state"] in ("crashed", "finished"):
                raise ApiError(408, "CASHOUT_TOO_LATE")
            if r["state"] != "running":
                raise ApiError(400, "ROUND_NOT_RUNNING")
            m = mult_at(now_ms() - r["started_at"])
            if m >= r["cp"]:
                raise ApiError(408, "CASHOUT_TOO_LATE")
            mult, ts = max(floor2(m), D("1.00")), now_ms()
            bet["settling"] = True                 # FINAL decision, taken under the same lock the crash uses
    if replay:
        res = await php("crash_int_bet_settle", {"bet_id": bid, "user_id": uid, "multiplier": "1.00", "cashout_key": key,
                                                 "cashed_out_at": now_ms(), "replay_only": True}, 6)
        bt = res["bet"]
        return {"success": True, "cashout": {"bet_id": bid, "round_id": bt["round_id"], "multiplier": float(bt["cashout_multiplier"]),
                "payout": float(bt["payout"]), "currency": bt["currency"], "cashed_out_at": int(bt["cashed_out_at"])}, **res["balances"]}
    task = spawn(settle_persist(bet, mult, key, ts))
    try:
        res = await asyncio.wait_for(asyncio.shield(task), 8)
    except asyncio.TimeoutError:
        raise ApiError(503, "CRASH_BACKEND_UNAVAILABLE")   # keeps retrying in background; idempotent
    log.info("[CRASH] cashout accepted")
    await broadcast(cashout_event(bet))
    return resp(bet, res)

@app.post("/crash/my-bets")
async def r_my_bets(request: Request):
    b = await jbody(request)
    _, uid, _ = who(b)
    return await php("crash_int_my_bets", {"user_id": uid, "limit": min(max(int(b.get("limit") or 20), 1), 100)})

@app.post("/crash/round-history")
async def r_history(request: Request):
    b = await jbody(request)
    who(b)
    return await php("crash_int_history", {"limit": min(max(int(b.get("limit") or 20), 1), 100)})

@app.post("/crash/round-details")
async def r_details(request: Request):
    b = await jbody(request)
    who(b)
    return await php("crash_int_round_details", {"round_id": str(b.get("round_id") or "")})

@app.websocket("/ws/crash")
async def ws_crash(ws: WebSocket):
    await ws.accept()
    try:
        msg = json.loads(await asyncio.wait_for(ws.receive_text(), 10))
        t = check_ticket(msg.get("ticket", "")) if msg.get("type") == "auth" else None
        if not t:
            await ws.send_text(json.dumps({"type": "auth_error", "error": "UNAUTHORIZED"}))
            await ws.close(code=4001)
            return
        if not st.ready or st.round is None:
            await ws.close(code=1013)
            return
        clients[ws] = int(t["uid"])
        log.info("[CRASH] websocket connected")
        await ws.send_text(json.dumps(snapshot_for(int(t["uid"]))))
        while True:
            await ws.receive_text()
    except (WebSocketDisconnect, asyncio.TimeoutError, json.JSONDecodeError):
        pass
    except Exception:
        log.exception("[CRASH] ws error")
    finally:
        if clients.pop(ws, None) is not None:
            log.info("[CRASH] websocket disconnected")