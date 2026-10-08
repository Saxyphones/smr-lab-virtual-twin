import asyncio
import json
import sys
import websockets

# Port from the command line: `python test_client.py 8766` for the IMU bridge
PORT = sys.argv[1] if len(sys.argv) > 1 else "8765"
URI = f"ws://localhost:{PORT}"

# Keys typed into the terminal -> session to replay
SESSION_KEYS = {"1": 451, "2": 452, "3": 453, "4": 454}


async def listen(ws):
    async for message in ws:
        print(message)


async def read_commands(ws):
    print(f"Type 1-4 then Enter to replay a session: {SESSION_KEYS}")
    while True:
        # input() blocks, so run it in a thread to keep receiving messages meanwhile
        key = (await asyncio.to_thread(input)).strip()
        if key not in SESSION_KEYS:
            print(f"Unknown key {key!r}; use one of {list(SESSION_KEYS)}.")
            continue
        command = {"command": "select_session", "sessionId": SESSION_KEYS[key]}
        print(f"Sending: {command}")
        await ws.send(json.dumps(command))


async def main():
    async with websockets.connect(URI) as ws:
        print(f"Connected to {URI}.")
        listener = asyncio.create_task(listen(ws))
        reader = asyncio.create_task(read_commands(ws))
        # Stop when either side ends (e.g. the server closes the connection)
        done, pending = await asyncio.wait({listener, reader}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in done:
            task.result()


if __name__ == "__main__":
    asyncio.run(main())
