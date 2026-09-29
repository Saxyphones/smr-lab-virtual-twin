import asyncio
import websockets
 
URI = "ws://localhost:8765"  # change to 8766 to test imu_bridge.py instead
 
 
async def main():
    async with websockets.connect(URI) as ws:
        print(f"Connected to {URI}. Waiting for messages...")
        async for message in ws:
            print(message)
 
 
if __name__ == "__main__":
    asyncio.run(main())
