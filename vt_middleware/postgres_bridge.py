import asyncio
import json
import asyncpg
import websockets
import os

DB_DSN = os.environ.get("DB_DSN")
TABLE_NAME = "robot"
FRAME_COLUMN = "frame_id"
JOINT_COLUMNS = ["joint_1", "joint_2", "joint_3", "joint_4", "joint_5", "joint_6"]
RECORDED_AT_COLUMN = "recorded_at"
SESSION_ID_COLUMN = "session_id"

POLL_INTERVAL_SECONDS = 0.05    # How often to check for new rows
SESSION_CHECK_INTERVAL_SECONDS = 2.0 # Needs to be replaced later
REPLAY_MAX_GAP_SECONDS = 2.0    # clamp long pauses in recorded history
REPLAY_SPEED = 1.0              # 1.0 = real recorded pace, 2.0 = 2x speed, etc.
ERROR_RETRY_SECONDS = 1.0       # back-off after a failed loop iteration (e.g. DB hiccup)
WEBSOCKET_HOST = "0.0.0.0"
WEBSOCKET_PORT = 8765


connected_clients = set()


class BridgeState:
    def __init__(self):
        self.current_session = None
        self.last_seen_frame = -1
        self.auto_follow = True     # False if client manually picks a session
        self.generation = 0

    def switch_session(self, session_id, auto: bool):
        self.current_session = session_id
        self.last_seen_frame = -1   # Reset cursor; re-fetched properly on the next poll
        self.auto_follow = auto
        mode = "auto-detected" if auto else "manually selected"
        print(f"Switching to session {session_id} ({mode}).")

state = BridgeState()

async def handle_client(websocket):
    # Keep track of connected Unity clients so the poll loop can broadcast to them
    connected_clients.add(websocket)
    print(f"Client connected. Total clients: {len(connected_clients)}")
    try:
        async for raw_message in websocket:
            # A bad message shouldn't drop this client's connection
            try:
                await handle_client_message(raw_message)
            except Exception as e:
                print(f"Error handling client message {raw_message!r}: {type(e).__name__}: {e}")
    finally:
        connected_clients.discard(websocket)
        print(f"Client disconnected. Total clients: {len(connected_clients)}")

async def handle_client_message(raw_message: str):
    try:
        message = json.loads(raw_message)
    except json.JSONDecodeError:
        print(f"Ignoring malformed client message: {raw_message!r}")
        return

    if not isinstance(message, dict):
        print(f"Ignoring non-object client message: {raw_message!r}")
        return

    command = message.get("command")

    if command == "select_session":
        # Accept 418 or "418"; reject missing/non-numeric values before they reach asyncpg
        try:
            session_id = int(message.get("sessionId"))
        except (TypeError, ValueError):
            print(f"select_session needs an integer 'sessionId', got {message.get('sessionId')!r}; ignoring.")
            return
        state.switch_session(session_id, auto=False)
        await broadcast_session_changed()

    elif command == "use_latest":
        print("Client requested auto-follow mode.")
        state.auto_follow = True
        state.generation += 1 # stops any currently-running replay segment
        if state.current_session is not None:
            # Jump to "now" rather than resuming from wherever replay left off
            state.last_seen_frame = await get_latest_frame_id(db_conn, state.current_session)
        await broadcast_session_changed()

    else:
        print(f"Ignoring unknown command: {command!r}")

async def broadcast(payload: dict):
    if not connected_clients:
        return
    message = json.dumps(payload)
    # Send to all connected clients concurrently; drop any that fail
    results = await asyncio.gather(
        *(client.send(message) for client in list (connected_clients)),
        return_exceptions=True,
    )
    for client, result in zip(list(connected_clients), results):
        if isinstance(result, Exception):
            connected_clients.discard(client)

async def broadcast_session_changed():
    await broadcast({
        "type": "session_changed",
        "sessionId": state.current_session,
        "autoFollow": state.auto_follow
    })

async def get_latest_session_id(conn):
    return await conn.fetchval(f"SELECT MAX({SESSION_ID_COLUMN}) FROM {TABLE_NAME}")


async def get_latest_frame_id(conn, session_id):
    """starts from the most recent frame within this specific session, not the whole table"""
    result = await conn.fetchval(
        f"SELECT MAX({FRAME_COLUMN}) FROM {TABLE_NAME} WHERE {SESSION_ID_COLUMN} = $1", session_id
    )
    return result if result is not None else -1

def row_to_data_payload(row):
    return {
        "type": "data",
        "sessionId": state.current_session,
        "frameId": row[FRAME_COLUMN],
        "joints": [row[col] for col in JOINT_COLUMNS],
        "recordedAt": row[RECORDED_AT_COLUMN]
    }

async def session_watcher(conn):
    """Runs continuously. Only acts while auto_follow is enabled."""
    while True:
        await asyncio.sleep(SESSION_CHECK_INTERVAL_SECONDS)
        if not state.auto_follow:
            continue
        try:
            latest = await get_latest_session_id(conn)
            if latest is not None and latest != state.current_session:
                state.switch_session(latest, auto=True)
                state.last_seen_frame = await get_latest_frame_id(conn, latest)
                await broadcast_session_changed()
        except Exception as e:
            # Skip this check; the next one runs after the normal interval
            print(f"Session watcher error: {type(e).__name__}: {e}")

async def run_live_segment(conn, my_generation):
    columns_sql = ", ".join([FRAME_COLUMN] + JOINT_COLUMNS + [RECORDED_AT_COLUMN])
    query = f"""
        SELECT {columns_sql}
        FROM {TABLE_NAME}
        WHERE {SESSION_ID_COLUMN} = $1 AND {FRAME_COLUMN} > $2
        ORDER BY {FRAME_COLUMN} ASC
    """

    while state.generation == my_generation:
        if state.current_session is None:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            continue

        rows = await conn.fetch(query, state.current_session, state.last_seen_frame)

        for row in rows:
            if state.generation != my_generation:
                return
            await broadcast(row_to_data_payload(row))
            state.last_seen_frame = row[FRAME_COLUMN]

        await asyncio.sleep(POLL_INTERVAL_SECONDS)

async def run_replay_segment(conn, my_generation):
    """Fetches the whole selected session once, then paces playback
    using real recorded_at gaps, looping at the end. Returns as soon
    as the generation changes."""
    if state.current_session is None:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        return

    columns_sql = ", ".join([FRAME_COLUMN] + JOINT_COLUMNS + [RECORDED_AT_COLUMN])
    query = f"""
        SELECT {columns_sql}
        FROM {TABLE_NAME}
        WHERE {SESSION_ID_COLUMN} = $1
        ORDER BY {FRAME_COLUMN} ASC
    """
    rows = await conn.fetch(query, state.current_session)
 
    if not rows:
        print(f"Session {state.current_session} has no rows to replay.")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        return
 
    print(f"Replaying session {state.current_session}: {len(rows)} frames.")
 
    while state.generation == my_generation:
        for i, row in enumerate(rows):
            if state.generation != my_generation:
                return
 
            if i > 0:
                gap = row[RECORDED_AT_COLUMN] - rows[i - 1][RECORDED_AT_COLUMN]
                gap = max(0.0, min(gap, REPLAY_MAX_GAP_SECONDS)) / REPLAY_SPEED
                if gap > 0:
                    await asyncio.sleep(gap)
                if state.generation != my_generation:
                    return
 
            await broadcast(row_to_data_payload(row))
            state.last_seen_frame = row[FRAME_COLUMN]

        # Loops back to start once end is reached

async def supervisor(conn):
    while True:
        my_generation = state.generation
        try:
            if state.auto_follow:
                await run_live_segment(conn, my_generation)
            else:
                await run_replay_segment(conn, my_generation)
        except Exception as e:
            # Log and retry rather than letting one failure take down the bridge
            print(f"Supervisor error: {type(e).__name__}: {e}. Retrying in {ERROR_RETRY_SECONDS}s.")
            await asyncio.sleep(ERROR_RETRY_SECONDS)

async def run(conn):
    global db_conn
    db_conn = conn

    state.current_session = await get_latest_session_id(conn)
    if state.current_session is None:
        print("No sessions found yet. Waiting...")
    else:
        state.last_seen_frame = await get_latest_frame_id(conn, state.current_session)
        print(f"Starting on session {state.current_session} (auto-detected, live), "
              f"from frame_id={state.last_seen_frame}.")

    await asyncio.gather(
        session_watcher(conn),
        supervisor(conn)
    )

async def main():
    conn = await asyncpg.connect(DB_DSN)
    print("Connected to Postgres.")

    server = await websockets.serve(handle_client, WEBSOCKET_HOST, WEBSOCKET_PORT)
    print(f"WebSocket server listening on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")

    await asyncio.gather(
        server.wait_closed(),
        run(conn)
    )

if __name__ == "__main__":
    asyncio.run(main())