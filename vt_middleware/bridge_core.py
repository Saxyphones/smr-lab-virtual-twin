"""Shared engine for the Postgres -> WebSocket bridges.

Each bridge streams one data source (robot telemetry, IMUs, ...) to Unity on
its own port. Everything that doesn't depend on the source lives here:
session tracking, live vs. replay mode, interrupting a running mode when a
client sends a command, and broadcasting to clients. The source-specific
parts (which table, how far the live stream has got, what a message looks
like) are a DataSource subclass - see postgres_bridge.py and imu_bridge.py.
"""
import asyncio
import json
import os
from pathlib import Path

import asyncpg
import websockets
from dotenv import load_dotenv

# Explicit path: load_dotenv() with no arguments searches from the current
# working directory when run under a debugger, and can miss this file
load_dotenv(Path(__file__).with_name(".env"))

POLL_INTERVAL_SECONDS = 0.05    # How often to check for new rows
SESSION_CHECK_INTERVAL_SECONDS = 2.0 # How often to check for a new session
ERROR_RETRY_SECONDS = 1.0       # back-off after a failed loop iteration (e.g. DB hiccup)
DB_POOL_MIN_SIZE = 2            # each bridge's supervisor + session watcher query concurrently
DB_POOL_MAX_SIZE = 10           # headroom for several bridges + client commands arriving mid-query
WEBSOCKET_HOST = "0.0.0.0"

# Sessions are registered here by the dashboard's "Start Session" button,
# before the robot is enabled - so a new session is visible well before its
# first data rows arrive
SESSION_TABLE_NAME = '"session"'
SESSION_TABLE_ID_COLUMN = "id"


async def get_latest_session_id(pool):
    """Newest registered session, whether or not it has data yet"""
    return await pool.fetchval(f"SELECT MAX({SESSION_TABLE_ID_COLUMN}) FROM {SESSION_TABLE_NAME}")


class DataSource:
    """The source-specific half of a bridge.

    The "cursor" is whatever the live stream needs to remember about how far
    it has got (a frame_id, a row id, ...). The engine never looks inside it;
    it just hands it back to the source.
    """
    name = "source"         # used as the log prefix
    rows_per_message = 1    # rows grouped into each "data" message

    def initial_cursor(self):
        """Cursor that streams a session from its very first row."""
        raise NotImplementedError

    async def latest_cursor(self, pool, session_id):
        """Cursor just past the session's newest row ("jump to now")."""
        raise NotImplementedError

    async def fetch_new(self, pool, session_id, cursor):
        """Rows newer than the cursor, oldest first."""
        raise NotImplementedError

    def advance(self, cursor, row):
        """Cursor after `row` has been sent."""
        raise NotImplementedError

    async def fetch_session(self, pool, session_id):
        """The whole session for replay, in playback order."""
        raise NotImplementedError

    def to_message(self, session_id, rows):
        """The "data" message sent to clients for these rows."""
        raise NotImplementedError


class Bridge:
    """One data source served on one WebSocket port."""

    def __init__(self, source: DataSource, port: int):
        self.source = source
        self.port = port
        self.pool = None
        self.clients = set()

        self.current_session = None
        self.cursor = source.initial_cursor()
        self.auto_follow = True     # False if client manually picks a session
        # Bumped on every session/mode change; a running live or replay loop
        # exits as soon as it sees the value it started with is stale
        self.generation = 0

    def log(self, message):
        print(f"[{self.source.name}] {message}")

    def switch_session(self, session_id, auto: bool):
        self.current_session = session_id
        self.cursor = self.source.initial_cursor()   # stream from the session's first row
        self.auto_follow = auto
        mode = "auto-detected" if auto else "manually selected"
        self.log(f"Switching to session {session_id} ({mode}).")

    # --- Clients ---

    async def handle_client(self, websocket):
        # Keep track of connected Unity clients so the poll loop can broadcast to them
        self.clients.add(websocket)
        self.log(f"Client connected. Total clients: {len(self.clients)}")
        try:
            async for raw_message in websocket:
                # A bad message shouldn't drop this client's connection
                try:
                    await self.handle_client_message(raw_message)
                except Exception as e:
                    self.log(f"Error handling client message {raw_message!r}: {type(e).__name__}: {e}")
        finally:
            self.clients.discard(websocket)
            self.log(f"Client disconnected. Total clients: {len(self.clients)}")

    async def handle_client_message(self, raw_message: str):
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            self.log(f"Ignoring malformed client message: {raw_message!r}")
            return

        if not isinstance(message, dict):
            self.log(f"Ignoring non-object client message: {raw_message!r}")
            return

        command = message.get("command")

        if command == "select_session":
            # Accept 418 or "418"; reject missing/non-numeric values before they reach asyncpg
            try:
                session_id = int(message.get("sessionId"))
            except (TypeError, ValueError):
                self.log(f"select_session needs an integer 'sessionId', got {message.get('sessionId')!r}; ignoring.")
                return
            self.switch_session(session_id, auto=False)
            self.generation += 1 # stops the live segment or any previous replay segment
            await self.broadcast_session_changed()

        elif command == "use_latest":
            self.log("Client requested auto-follow mode.")
            await self.switch_to_latest()

        else:
            self.log(f"Ignoring unknown command: {command!r}")

    async def switch_to_latest(self):
        """Enters live (auto-follow) mode on the newest session, starting from
        its newest row. Shared by the use_latest command and the replay
        fallback for empty sessions."""
        self.auto_follow = True
        self.generation += 1 # stops any currently-running replay segment
        latest = await get_latest_session_id(self.pool)
        if latest is not None:
            if latest != self.current_session:
                self.switch_session(latest, auto=True)
            # Jump to "now" rather than resuming from wherever replay left off
            self.cursor = await self.source.latest_cursor(self.pool, latest)
        await self.broadcast_session_changed()

    async def broadcast(self, payload: dict):
        if not self.clients:
            return
        message = json.dumps(payload)
        # Send to all connected clients concurrently; drop any that fail
        clients = list(self.clients)
        results = await asyncio.gather(
            *(client.send(message) for client in clients),
            return_exceptions=True,
        )
        for client, result in zip(clients, results):
            if isinstance(result, Exception):
                self.clients.discard(client)

    async def broadcast_session_changed(self):
        await self.broadcast({
            "type": "session_changed",
            "sessionId": self.current_session,
            "autoFollow": self.auto_follow
        })

    async def send_rows(self, session_id, rows, my_generation, advance_cursor: bool):
        """Broadcasts rows in source-sized chunks. Returns False if a newer
        command or session switch interrupted part-way through."""
        size = self.source.rows_per_message
        for start in range(0, len(rows), size):
            if self.generation != my_generation:
                return False
            chunk = rows[start:start + size]
            await self.broadcast(self.source.to_message(session_id, chunk))
            # Re-check after the await: a switch during the broadcast has
            # already reset the cursor, and these rows belong to the old session
            if advance_cursor and self.generation == my_generation:
                self.cursor = self.source.advance(self.cursor, chunk[-1])
        return True

    # --- Background loops ---

    async def session_watcher(self):
        """Runs continuously. Only acts while auto_follow is enabled."""
        while True:
            await asyncio.sleep(SESSION_CHECK_INTERVAL_SECONDS)
            if not self.auto_follow:
                continue
            try:
                latest = await get_latest_session_id(self.pool)
                if latest is not None and latest != self.current_session:
                    # switch_session resets the cursor, so the live segment
                    # streams this session from its very first row. A session
                    # found here is at most one check interval old, so any rows
                    # that already exist are the start of the motion, not stale history.
                    self.switch_session(latest, auto=True)
                    # Restart the live segment so rows already fetched for the
                    # old session can't be sent under, or move the cursor of, the new one
                    self.generation += 1
                    await self.broadcast_session_changed()
            except Exception as e:
                # Skip this check; the next one runs after the normal interval
                self.log(f"Session watcher error: {type(e).__name__}: {e}")

    async def run_live_segment(self, my_generation):
        while self.generation == my_generation:
            session_id = self.current_session
            if session_id is not None:
                rows = await self.source.fetch_new(self.pool, session_id, self.cursor)
                if not await self.send_rows(session_id, rows, my_generation, advance_cursor=True):
                    return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def run_replay_segment(self, my_generation):
        """Fetches the whole selected session once and broadcasts it as a single
        burst - no pacing, no looping. Unity owns replay pacing/looping, the
        same way CsvPlaybackAdapter paces a loaded CSV. After the burst, idles
        (cheaply) until a new command changes the generation - it must NOT just
        return here, since the supervisor would immediately call this again
        with nothing having changed, causing an infinite re-fetch loop."""
        session_id = self.current_session
        if session_id is None:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            return

        rows = await self.source.fetch_session(self.pool, session_id)
        if self.generation != my_generation:
            return # a newer command arrived during the fetch; let it win

        if not rows:
            self.log(f"Session {session_id} has no rows to send. Falling back to live mode.")
            await self.broadcast({
                "type": "session_empty",
                "sessionId": session_id,
            })
            # Bumps the generation, so the supervisor moves on to the live segment
            await self.switch_to_latest()
            return

        self.log(f"Sending session {session_id} to client(s) for local "
                 f"replay: {len(rows)} rows.")

        # The cursor only matters in live mode, and every route back to live
        # resets it, so replay doesn't advance it
        if not await self.send_rows(session_id, rows, my_generation, advance_cursor=False):
            return

        # Explicit end-of-burst marker, so the client knows when it's safe to
        # start replaying rather than guessing via a timeout
        await self.broadcast({
            "type": "replay_complete",
            "sessionId": session_id,
            "frameCount": len(rows),
        })

        # Whole session sent. Idle until interrupted by a new command - do not
        # return normally, or the supervisor will immediately re-fetch and
        # re-broadcast the same burst again.
        while self.generation == my_generation:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def supervisor(self):
        while True:
            my_generation = self.generation
            try:
                if self.auto_follow:
                    await self.run_live_segment(my_generation)
                else:
                    await self.run_replay_segment(my_generation)
            except Exception as e:
                # Log and retry rather than letting one failure take down the bridge
                self.log(f"Supervisor error: {type(e).__name__}: {e}. Retrying in {ERROR_RETRY_SECONDS}s.")
                await asyncio.sleep(ERROR_RETRY_SECONDS)

    async def run(self, pool):
        self.pool = pool

        self.current_session = await get_latest_session_id(pool)
        if self.current_session is None:
            self.log("No sessions found yet. Waiting...")
        else:
            self.cursor = await self.source.latest_cursor(pool, self.current_session)
            self.log(f"Starting on session {self.current_session} (auto-detected, live), "
                     f"from cursor {self.cursor}.")

        async with websockets.serve(self.handle_client, WEBSOCKET_HOST, self.port):
            self.log(f"WebSocket server listening on ws://{WEBSOCKET_HOST}:{self.port}")
            await asyncio.gather(
                self.session_watcher(),
                self.supervisor(),
            )


async def serve(*bridges: Bridge):
    """Runs one or more bridges in this process, sharing one connection pool."""
    dsn = os.environ.get("DB_DSN")
    if not dsn:
        # Without this check asyncpg silently falls back to localhost:5432,
        # which fails with a confusing "connection refused"
        raise SystemExit(f"DB_DSN is not set. Put it in {Path(__file__).with_name('.env')}.")

    # A pool rather than a single connection: the supervisor, session watcher
    # and client command handler all query concurrently, and one asyncpg
    # connection can only run one query at a time. pool.fetch()/fetchval()
    # each borrow a free connection for the duration of the query.
    async with asyncpg.create_pool(dsn, min_size=DB_POOL_MIN_SIZE, max_size=DB_POOL_MAX_SIZE) as pool:
        print("Connected to Postgres.")
        await asyncio.gather(*(bridge.run(pool) for bridge in bridges))
