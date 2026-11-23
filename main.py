"""
RANKCO online server  (FastAPI + WebSockets + Neon Postgres)  -  single file.

Deploy on Render
----------------
  requirements.txt :  fastapi
                      uvicorn[standard]
                      asyncpg
  Build command    :  pip install -r requirements.txt
  Start command    :  uvicorn main:app --host 0.0.0.0 --port $PORT
  Environment      :  DATABASE_URL      (Neon connection string - required for persistence)
                      ALLOWED_ORIGINS   (optional, comma separated, default "*")

How it works
------------
The game engine in index.html is deterministic, so the server does not re-implement the rules.
It is the *referee of order and luck*:
  * it hosts the rooms (5-letter code, 2 seats) and stores every move in Neon,
  * it accepts a move only from the player it belongs to, only in the right sequence number,
  * it rolls the dice itself (secure RNG) - clients cannot choose their roll,
  * it relays each accepted move to both players, who apply it with the same engine,
  * a player who reconnects (or a server that restarts) gets the full move log and replays it.

If DATABASE_URL is missing or Neon is unreachable the server keeps working in memory only.
"""
import asyncio
import json
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

log = logging.getLogger("rankco")
logging.basicConfig(level=logging.INFO)

COLORS = ["blue", "red", "green", "orange"]            # same order as the engine
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"      # no 0/O/1/I
CODE_LEN = 5
ALLOWED_ACTIONS = {
    "roll", "enter", "enter_pay", "move", "unfreeze", "unfreeze_pay",
    "freeze", "recover", "pass", "setback_pay", "setback_accept",
}
MAX_ROOMS = 5000
MEMORY_TTL = 6 * 3600           # drop idle rooms from memory after 6 h (they stay in Neon)
DB_TTL_DAYS = 3                 # delete rooms from Neon after 3 days without activity

POOL: Optional[asyncpg.Pool] = None
ROOMS: "dict[str, Room]" = {}
LOAD_LOCK = asyncio.Lock()


# ----------------------------------------------------------------------------- database
def make_dsn(url: str):
    """Neon URLs carry sslmode / channel_binding query params that asyncpg does not take."""
    parts = urlsplit(url)
    q = [(k, v) for k, v in parse_qsl(parts.query) if k not in ("sslmode", "channel_binding")]
    return urlunsplit(parts._replace(query=urlencode(q))), parts.hostname or ""


SCHEMA = """
CREATE TABLE IF NOT EXISTS rankco_rooms (
    code        TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    seats       JSONB NOT NULL,
    cfg         JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS rankco_moves (
    code    TEXT NOT NULL REFERENCES rankco_rooms(code) ON DELETE CASCADE,
    seq     INT  NOT NULL,
    pid     INT  NOT NULL,
    action  JSONB NOT NULL,
    dice    INT,
    PRIMARY KEY (code, seq)
);
"""


async def init_db():
    global POOL
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        log.warning("DATABASE_URL not set - running in memory-only mode")
        return
    dsn, host = make_dsn(url)
    try:
        POOL = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            ssl=False if host in ("localhost", "127.0.0.1") else "require",
            statement_cache_size=0,                 # Neon's pooled endpoint (pgbouncer) needs this
            max_inactive_connection_lifetime=120,   # Neon suspends idle connections
            command_timeout=30,
            timeout=40,                             # first connect can wake a suspended Neon compute
        )
        async with POOL.acquire() as con:
            await con.execute(SCHEMA)
        log.info("Neon connected")
    except Exception:
        log.exception("Could not connect to the database - running in memory-only mode")
        POOL = None


async def db_exec(sql: str, *args):
    if not POOL:
        return
    try:
        await POOL.execute(sql, *args)
    except Exception:
        log.exception("db write failed")


# ----------------------------------------------------------------------------- rooms
class Room:
    def __init__(self, code, status, seats, cfg=None, moves=None):
        self.code = code
        self.status = status            # waiting | playing | closed
        self.seats = seats              # [{name, color, token}]
        self.cfg = cfg                  # engine config once two players are seated
        self.log = moves or []          # [{seq, pid, action, dice}]
        self.sockets: "dict[int, WebSocket]" = {}
        self.lock = asyncio.Lock()
        self.touched = time.time()


def pid_of(room: Room, seat: int) -> Optional[int]:
    """Player id the engine gives this seat: players are sorted by colour order."""
    if len(room.seats) < 2:
        return None
    order = sorted(range(len(room.seats)), key=lambda i: COLORS.index(room.seats[i]["color"]))
    return order.index(seat)


def seats_public(room: Room):
    return [
        {"name": s["name"], "color": s["color"], "pid": pid_of(room, i), "online": i in room.sockets}
        for i, s in enumerate(room.seats)
    ]


def state_for(room: Room, seat: int, force: bool = False):
    return {
        "type": "state", "code": room.code, "status": room.status, "seat": seat,
        "pid": pid_of(room, seat), "seats": seats_public(room), "cfg": room.cfg,
        "log": room.log, "force": force,
    }


async def send_safe(ws: WebSocket, payload: dict):
    try:
        await ws.send_json(payload)
    except Exception:
        pass


async def broadcast(room: Room, payload: dict):
    await asyncio.gather(*[send_safe(ws, payload) for ws in list(room.sockets.values())])


async def broadcast_state(room: Room):
    await asyncio.gather(*[send_safe(ws, state_for(room, seat)) for seat, ws in list(room.sockets.items())])


async def load_room(code: str) -> Optional[Room]:
    if not POOL:
        return None
    try:
        row = await POOL.fetchrow("SELECT status, seats, cfg FROM rankco_rooms WHERE code=$1", code)
        if not row:
            return None
        rows = await POOL.fetch("SELECT seq, pid, action, dice FROM rankco_moves WHERE code=$1 ORDER BY seq", code)
    except Exception:
        log.exception("db read failed")
        return None
    moves = [{"seq": r["seq"], "pid": r["pid"], "action": json.loads(r["action"]), "dice": r["dice"]} for r in rows]
    if any(m["seq"] != i for i, m in enumerate(moves)):
        moves = moves[: next(i for i, m in enumerate(moves) if m["seq"] != i)]
    return Room(code, row["status"], json.loads(row["seats"]), json.loads(row["cfg"]) if row["cfg"] else None, moves)


async def get_room(code: str) -> Optional[Room]:
    code = (code or "").strip().upper()
    room = ROOMS.get(code)
    if room:
        room.touched = time.time()
        return room
    async with LOAD_LOCK:
        room = ROOMS.get(code)
        if not room:
            room = await load_room(code)
            if room:
                ROOMS[code] = room
    if room:
        room.touched = time.time()
    return room


def clean_name(name: str, fallback: str) -> str:
    n = " ".join((name or "").split())[:14]
    return n or fallback


def clean_color(color: str) -> str:
    return color if color in COLORS else "blue"


def clean_action(a) -> Optional[dict]:
    if not isinstance(a, dict) or a.get("type") not in ALLOWED_ACTIONS:
        return None
    out = {"type": a["type"]}
    if "token" in a:
        t = a["token"]
        if isinstance(t, bool) or t not in (0, 1):
            return None
        out["token"] = t
    if a.get("paid") is True:
        out["paid"] = True
    return out


# ----------------------------------------------------------------------------- app
async def janitor():
    while True:
        await asyncio.sleep(1800)
        now = time.time()
        for code, room in list(ROOMS.items()):
            if not room.sockets and now - room.touched > MEMORY_TTL:
                ROOMS.pop(code, None)
        await db_exec(f"DELETE FROM rankco_rooms WHERE updated_at < now() - interval '{DB_TTL_DAYS} days'")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    task = asyncio.create_task(janitor())
    yield
    task.cancel()
    if POOL:
        await POOL.close()


app = FastAPI(title="RANKCO online", lifespan=lifespan)
origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["*"], allow_headers=["*"])


class RoomIn(BaseModel):
    name: str = ""
    color: str = "blue"


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"service": "rankco-online", "ok": True}


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"ok": True, "db": POOL is not None, "rooms": len(ROOMS)}


@app.post("/api/rooms")
async def create_room(body: RoomIn):
    if len(ROOMS) >= MAX_ROOMS:
        raise HTTPException(503, "Server is busy, try again in a minute")
    token = secrets.token_urlsafe(24)
    seat = {"name": clean_name(body.name, "Player 1"), "color": clean_color(body.color), "token": token}
    for _ in range(30):
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LEN))
        if code in ROOMS:
            continue
        if POOL:
            try:
                got = await POOL.fetchval(
                    "INSERT INTO rankco_rooms(code,status,seats) VALUES($1,'waiting',$2::jsonb) "
                    "ON CONFLICT DO NOTHING RETURNING code", code, json.dumps([seat]))
                if not got:
                    continue
            except Exception:
                log.exception("db insert failed - room kept in memory only")
        ROOMS[code] = Room(code, "waiting", [seat])
        return {"code": code, "token": token, "seat": 0}
    raise HTTPException(503, "Could not allocate a room code")


@app.post("/api/rooms/{code}/join")
async def join_room(code: str, body: RoomIn):
    room = await get_room(code)
    if not room:
        raise HTTPException(404, "Room not found - check the code")
    async with room.lock:
        if room.status != "waiting" or len(room.seats) >= 2:
            raise HTTPException(409, "That room is already full")
        host = room.seats[0]
        color = clean_color(body.color)
        if color == host["color"]:
            color = next(c for c in COLORS if c != host["color"])
        name = clean_name(body.name, "Player 2")
        if name.lower() == host["name"].lower():
            name = name[:12] + " 2"
        token = secrets.token_urlsafe(24)
        room.seats.append({"name": name, "color": color, "token": token})
        room.status = "playing"
        room.cfg = {
            "seed": secrets.randbelow(2**32),
            "players": [{"name": s["name"], "color": s["color"], "type": "human"} for s in room.seats],
            "startColor": secrets.choice([s["color"] for s in room.seats]),
        }
        await db_exec(
            "UPDATE rankco_rooms SET status='playing', seats=$2::jsonb, cfg=$3::jsonb, updated_at=now() WHERE code=$1",
            room.code, json.dumps(room.seats), json.dumps(room.cfg))
    await broadcast_state(room)         # tells the host (already in the lobby) that the game starts
    return {"code": room.code, "token": token, "seat": 1}


@app.websocket("/ws/{code}")
async def ws_endpoint(ws: WebSocket, code: str, token: str = ""):
    await ws.accept()
    room = await get_room(code)
    seat = None
    if room:
        seat = next((i for i, s in enumerate(room.seats) if secrets.compare_digest(s["token"], token or "")), None)
    if room is None or seat is None:
        await send_safe(ws, {"type": "error", "fatal": True, "error": "Room not found or invalid session"})
        await ws.close(code=4404)
        return

    old = room.sockets.get(seat)
    room.sockets[seat] = ws
    if old is not None and old is not ws:
        try:
            await old.close(code=4000)
        except Exception:
            pass
    await send_safe(ws, state_for(room, seat))
    await broadcast(room, {"type": "presence", "seats": seats_public(room)})

    try:
        while True:
            raw = await ws.receive_text()
            if len(raw) > 2000:
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            room.touched = time.time()
            kind = msg.get("type")
            if kind == "ping":
                await send_safe(ws, {"type": "pong"})
            elif kind == "sync":
                await send_safe(ws, state_for(room, seat, force=True))
            elif kind == "leave":
                async with room.lock:
                    if room.status != "closed":
                        room.status = "closed"
                        await db_exec("UPDATE rankco_rooms SET status='closed', updated_at=now() WHERE code=$1", room.code)
                await broadcast(room, {"type": "left", "seat": seat})
                break
            elif kind == "action":
                await handle_action(room, seat, ws, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("ws error")
    finally:
        if room.sockets.get(seat) is ws:
            room.sockets.pop(seat, None)
            await broadcast(room, {"type": "presence", "seats": seats_public(room)})
        try:
            await ws.close()
        except Exception:
            pass


async def handle_action(room: Room, seat: int, ws: WebSocket, msg: dict):
    async with room.lock:
        if room.status != "playing":
            await send_safe(ws, {"type": "reject", "reason": "status"})
            return
        pid = pid_of(room, seat)
        action = clean_action(msg.get("action"))
        seq = msg.get("seq")
        if action is None or msg.get("pid") != pid:
            await send_safe(ws, {"type": "reject", "reason": "invalid"})
            return
        if seq != len(room.log):                     # stale / duplicate / out of order
            await send_safe(ws, {"type": "reject", "reason": "seq", "expected": len(room.log)})
            return
        dice = secrets.randbelow(6) + 1 if action["type"] == "roll" else None
        entry = {"seq": seq, "pid": pid, "action": action, "dice": dice}
        room.log.append(entry)
        await broadcast(room, {"type": "action", **entry})
        if POOL:
            await db_exec(
                "WITH ins AS (INSERT INTO rankco_moves(code,seq,pid,action,dice) VALUES($1,$2,$3,$4::jsonb,$5) "
                "ON CONFLICT DO NOTHING RETURNING 1) UPDATE rankco_rooms SET updated_at=now() WHERE code=$1",
                room.code, seq, pid, json.dumps(action), dice)
