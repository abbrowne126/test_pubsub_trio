from .async_client import AsyncClient
from google.cloud.pubsub_v1.types import FlowControl
import trio

async def subscribe():
    # https://docs.cloud.google.com/pubsub/docs/flow-control-messages
    flow_control_settings = FlowControl(max_messages=10)
    # See https://anyio.readthedocs.io/en/stable/api.html#streams-and-stream-wrappers
    # size=0 will block send on a receive call.
    max_buffer_size = 0

    async with AsyncClient() as sub_client:
        async for msg in sub_client.subscribe(
            "projects/p/subscriptions/s",
            flow_control=flow_control_settings,
            max_buffer_size=max_buffer_size,
        ):
            print(msg)
            msg.ack()

def main():
  trio.run(subscribe)

if __name__ == "__main__":
  main()
