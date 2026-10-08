"""Robot-telemetry bridge: streams the `robot` table to Unity on port 8765.

The session/live/replay machinery is shared with the other bridges in
bridge_core.py; this file only describes the robot table and its messages.
"""
import asyncio

from bridge_core import Bridge, DataSource, serve

TABLE_NAME = "robot"
FRAME_COLUMN = "frame_id"
JOINT_COLUMNS = ["joint_1", "joint_2", "joint_3", "joint_4", "joint_5", "joint_6"]
RECORDED_AT_COLUMN = "recorded_at"
SESSION_ID_COLUMN = "session_id"
WEBSOCKET_PORT = 8765

COLUMNS_SQL = ", ".join([FRAME_COLUMN] + JOINT_COLUMNS + [RECORDED_AT_COLUMN])


class RobotSource(DataSource):
    """Cursor is the last sent frame_id. frame_id restarts in every session
    and is the robot's own ordering key."""
    name = "robot"
    rows_per_message = 1    # BridgeClient expects one frame per data message

    def initial_cursor(self):
        return -1   # not 0: some sources start frame_id at 0

    async def latest_cursor(self, pool, session_id):
        result = await pool.fetchval(
            f"SELECT MAX({FRAME_COLUMN}) FROM {TABLE_NAME} WHERE {SESSION_ID_COLUMN} = $1", session_id
        )
        return result if result is not None else -1

    async def fetch_new(self, pool, session_id, cursor):
        return await pool.fetch(f"""
            SELECT {COLUMNS_SQL}
            FROM {TABLE_NAME}
            WHERE {SESSION_ID_COLUMN} = $1 AND {FRAME_COLUMN} > $2
            ORDER BY {FRAME_COLUMN} ASC
        """, session_id, cursor)

    def advance(self, cursor, row):
        return row[FRAME_COLUMN]

    async def fetch_session(self, pool, session_id):
        return await pool.fetch(f"""
            SELECT {COLUMNS_SQL}
            FROM {TABLE_NAME}
            WHERE {SESSION_ID_COLUMN} = $1
            ORDER BY {FRAME_COLUMN} ASC
        """, session_id)

    def to_message(self, session_id, rows):
        row = rows[0]
        return {
            "type": "data",
            "sessionId": session_id,
            "frameId": row[FRAME_COLUMN],
            "joints": [row[col] for col in JOINT_COLUMNS],
            "recordedAt": row[RECORDED_AT_COLUMN]
        }


if __name__ == "__main__":
    asyncio.run(serve(Bridge(RobotSource(), WEBSOCKET_PORT)))
