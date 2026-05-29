# test_pubsub_trio

Experimental wrapper around the [Cloud Pub/Sub python client library](https://docs.cloud.google.com/python/docs/reference/pubsub/latest) that schedules user callback code on the user's async runtime eventloop. Through the [AnyIO library](https://anyio.readthedocs.io/en/stable/), both [asyncio](https://docs.python.org/3/library/asyncio.html) and [trio](https://trio.readthedocs.io) are supported.

Has a similar API to the existing client, as it wraps the existing multithreaded but synchronous client. Callbacks are scheduled on the user's event loop through a [blocking portal](https://anyio.readthedocs.io/en/stable/threads.html#running-code-from-threads-using-blocking-portals):

```python
async with BlockingPortal() as portal:
  def callback(message: Message) -> None:
    portal.call(send_stream.send, message)

  self.__streaming_pull_future = self.__client.subscribe(
      subscription, callback=callback, flow_control=flow_control
  )

  async with receive_stream:
    async for message in receive_stream:
      yield message
```


# Example Usage
```python
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
```
