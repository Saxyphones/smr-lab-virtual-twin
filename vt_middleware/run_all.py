"""Runs every bridge in one process (robot on 8765, IMU on 8766), sharing one
database connection pool. Each bridge can still be run on its own with
`python postgres_bridge.py` or `python imu_bridge.py`."""
import asyncio

import imu_bridge
import postgres_bridge
from bridge_core import Bridge, serve

if __name__ == "__main__":
    asyncio.run(serve(
        Bridge(postgres_bridge.RobotSource(), postgres_bridge.WEBSOCKET_PORT),
        Bridge(imu_bridge.ImuSource(), imu_bridge.WEBSOCKET_PORT),
    ))
