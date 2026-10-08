"""IMU bridge: streams `imu_measurement` to Unity on port 8766.

Same session/live/replay behavior and commands as the robot bridge (see
bridge_core.py). Differences from the robot stream:
- Several IMUs share one session, so data messages carry a batch of samples,
  each tagged with its deviceId and the robot joint it is mounted on.
- Orientation is sent as a quaternion as well as the raw yaw/pitch/roll.
- recordedAt is converted from the table's milliseconds to seconds, matching
  the robot stream.
"""
import asyncio
import math

from bridge_core import Bridge, DataSource, serve

TABLE_NAME = "imu_measurement"
SESSION_ID_COLUMN = "session_id"
WEBSOCKET_PORT = 8766

# device.id -> robot joint the IMU is mounted on. No J6 IMU yet.
DEVICE_TO_JOINT = {280: 1, 281: 2, 282: 3, 279: 4, 283: 5}

COLUMNS_SQL = "id, device_id, frame_id, recorded_at, yaw, pitch, roll"


def ypr_to_quaternion(yaw, pitch, roll):
    """Degrees -> (w, x, y, z) for R = Rz(yaw) * Ry(pitch) * Rx(roll).

    That Z-Y-X order was verified against the quaternions in imu_logs. The
    quaternion is in the IMU's own right-handed frame; converting to Unity's
    left-handed axes is the client's job.
    """
    # Each rotation contributes a half-angle cos/sin pair
    cy, sy = math.cos(math.radians(yaw) / 2), math.sin(math.radians(yaw) / 2)
    cp, sp = math.cos(math.radians(pitch) / 2), math.sin(math.radians(pitch) / 2)
    cr, sr = math.cos(math.radians(roll) / 2), math.sin(math.radians(roll) / 2)
    return (
        cr * cp * cy + sr * sp * sy,
        sr * cp * cy - cr * sp * sy,
        cr * sp * cy + sr * cp * sy,
        cr * cp * sy - sr * sp * cy,
    )


class ImuSource(DataSource):
    """Cursor is the last sent row id. id comes from a database sequence, so
    it follows insert order across all IMUs (checked on session 503: no
    out-of-order rows), and `id > cursor` can use the primary key index."""
    name = "imu"
    # ~5 IMUs at ~100 Hz: a live poll returns ~25 rows, a replay ~15-20k.
    # Batching keeps that to a sensible number of messages.
    rows_per_message = 100

    def __init__(self):
        self.warned_devices = set()

    def initial_cursor(self):
        return -1

    async def latest_cursor(self, pool, session_id):
        result = await pool.fetchval(
            f"SELECT MAX(id) FROM {TABLE_NAME} WHERE {SESSION_ID_COLUMN} = $1", session_id
        )
        return result if result is not None else -1

    async def fetch_new(self, pool, session_id, cursor):
        return await pool.fetch(f"""
            SELECT {COLUMNS_SQL}
            FROM {TABLE_NAME}
            WHERE {SESSION_ID_COLUMN} = $1 AND id > $2
            ORDER BY id ASC
        """, session_id, cursor)

    def advance(self, cursor, row):
        return row["id"]

    async def fetch_session(self, pool, session_id):
        # Playback order across all IMUs is by recording time, not insert order
        return await pool.fetch(f"""
            SELECT {COLUMNS_SQL}
            FROM {TABLE_NAME}
            WHERE {SESSION_ID_COLUMN} = $1
            ORDER BY recorded_at ASC, id ASC
        """, session_id)

    def to_message(self, session_id, rows):
        return {
            "type": "data",
            "sessionId": session_id,
            "samples": [self.to_sample(row) for row in rows],
        }

    def to_sample(self, row):
        device_id = row["device_id"]
        joint = DEVICE_TO_JOINT.get(device_id, 0)
        if joint == 0 and device_id not in self.warned_devices:
            self.warned_devices.add(device_id)
            print(f"[{self.name}] Device {device_id} isn't in DEVICE_TO_JOINT; sending it with joint 0.")
        w, x, y, z = ypr_to_quaternion(row["yaw"], row["pitch"], row["roll"])
        return {
            "deviceId": device_id,
            "joint": joint,                         # 0 = not mapped to a joint
            "frameId": row["frame_id"],             # counts per device
            "recordedAt": row["recorded_at"] / 1000.0,  # ms in the table -> seconds
            "quat": [w, x, y, z],
            "ypr": [row["yaw"], row["pitch"], row["roll"]],  # degrees, for debugging
        }


if __name__ == "__main__":
    asyncio.run(serve(Bridge(ImuSource(), WEBSOCKET_PORT)))
