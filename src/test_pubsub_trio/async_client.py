
from collections.abc import AsyncGenerator
from contextlib import suppress
from typing import Any, Optional, Sequence, Union

import anyio
from anyio.from_thread import BlockingPortal
from google.cloud.pubsub_v1 import types
from google.cloud.pubsub_v1.exceptions import TimeoutError
from google.api_core.exceptions import Cancelled as CancelledError
from google.cloud.pubsub_v1.subscriber import futures
from google.cloud.pubsub_v1.subscriber.client import Client as SyncClient
from google.cloud.pubsub_v1.subscriber.message import Message
from google.oauth2 import service_account


class AsyncClient:
  """An async-friendly subscriber client for Google Cloud Pub/Sub.

  Uses anyio for compatibility with asyncio and trio.

  Args:
      kwargs: Any additional arguments provided are sent as keyword keyword
        arguments to the underlying
        :class:`~google.cloud.pubsub_v1.subscriber_client.SubscriberClient`.
        Generally you should not need to set additional keyword arguments.
        Optionally, regional endpoints can be set via ``client_options`` that
        takes a single key-value pair that defines the endpoint.
  """

  def __init__(
      self,
      subscriber_options: Union[types.SubscriberOptions, Sequence] = (),
      **kwargs: Any
  ):
    self.__client = SyncClient(subscriber_options, **kwargs)
    self.__streaming_pull_future = None

  async def __aenter__(self):
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    await self.close()

  @property
  def open_telemetry_enabled(self) -> bool:
    return self.__client.open_telemetry_enabled

  @classmethod
  def from_service_account_file(
      cls, filename: str, **kwargs: Any
  ) -> "AsyncClient":
    """Creates an instance of this client using the provided credentials

    file.

    Args:
        filename: The path to the service account private key json file.
        kwargs: Additional arguments to pass to the constructor.

    Returns:
        A Subscriber
        instance that is the constructed client.
    """
    credentials = service_account.Credentials.from_service_account_file(
        filename
    )
    kwargs["credentials"] = credentials
    return cls(**kwargs)

  @property
  def target(self) -> str:
    """Return the target (where the API is).

    Returns:
        The location of the API.
    """
    return self.__client.target

  @property
  def closed(self) -> bool:
    """Return whether the client has been closed and cannot be used anymore.

    .. versionadded:: 2.8.0
    """
    return self.__client.closed

  async def close(self, timeout: Optional[float] = None) -> None:
    """Close the subscriber client.

    Args:
      timeout: The timeout in seconds to wait for the client to close.
    """
    if self.closed:
      return

    def close_sync(event: anyio.Event):
      self.__client.close()

      if self.__streaming_pull_future is not None:
        with suppress(CancelledError):
          try:
            self.__streaming_pull_future.result(timeout=timeout)
          except TimeoutError:
            self.__streaming_pull_future.cancel()
            self.__streaming_pull_future.result()

      anyio.from_thread.run_sync(event.set)

    event = anyio.Event()
    await anyio.to_thread.run_sync(close_sync, event)
    await event.wait()

  @property
  def stream_future(self) -> futures.StreamingPullFuture:
    """Retrieve the streaming pull future.

    Returns:
        The streaming pull future.
    """
    return self.__streaming_pull_future # type: ignore

  async def subscribe(
      self,
      subscription: str,
      flow_control: Union[types.FlowControl, Sequence] = (),
      max_buffer_size: int = 0,
  ) -> AsyncGenerator[Message, None]:
    """Subscribe to a Pub/Sub subscription with a streaming pull connection.

    Asynchronously yields messages from the stream.

    Args:
      subscription: The subscription to subscribe to.
      flow_control: The flow control settings for the subscription.
      max_buffer_size: The maximum buffer size for the stream. See [anyio
        streams](https://anyio.readthedocs.io/en/stable/streams.html) for more
        information.

    Yields:
        Messages from the subscription.
    """

    send_stream, receive_stream = anyio.create_memory_object_stream[Message](
        max_buffer_size=max_buffer_size
    )

    # A BlockingPortal is needed to run code on the user's existing event loop.
    # For more information, see
    # https://anyio.readthedocs.io/en/stable/threads.html#running-code-from-threads-using-blocking-portals
    async with BlockingPortal() as portal:

      def callback(message: Message) -> None:
        portal.call(send_stream.send, message)

      self.__streaming_pull_future = self.__client.subscribe(
          subscription, callback=callback, flow_control=flow_control
      )

      async with receive_stream:
        async for message in receive_stream:
          yield message
