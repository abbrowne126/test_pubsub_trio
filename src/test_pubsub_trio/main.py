from async_client import AsyncClient
import trio

async def subscribe():
  async with AsyncClient() as sub_client:
    async for msg in sub_client.subscribe("projects/p/subscriptions/s"):
      print(msg)
      msg.ack()

def main():
  trio.run(subscribe)

if __name__ == "__main__":
  main()
