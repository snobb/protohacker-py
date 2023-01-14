import asyncio
import logging
import os
import sys

# 04. Unusual database - https://protohackers.com/problem/4

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG", None) else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("UDP_PORT", "5000"))

Address = tuple[str, int]


class UnusualDB(asyncio.DatagramProtocol):
    version = 'Odd database v1.0'

    def __init__(self, logger: logging.Logger) -> None:
        self.log = logger
        self.store: dict[str, str] = {}

    # transport should be asyncio.DatagramTransport, but that causes mypy
    # type error in self.send.
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Address) -> None:
        msg = data.decode()

        self.log.debug(f"data: {msg}, addr: {addr}")

        if len(msg) > 1000:
            self.log.error(f"receive: message is too big: {msg}")

        key, value, is_insert = self.parse(msg)

        if key == "version":
            self.send(f"version={UnusualDB.version}", addr)
            return

        if is_insert:
            self.log.debug(f"insert: {key}={value}")
            self.store[key] = value

        else:
            self.log.debug(f"query: {key}={self.store.get(key, '')}")
            self.send(f"{key}={self.store.get(key, '')}", addr)

    def send(self, msg: str, addr: Address) -> None:
        # hack to shut the mypy hack about transport not being
        # DatagramTransport in connection_made.
        assert (isinstance(self.transport, asyncio.DatagramTransport))

        if len(msg) > 1000:
            self.log.error(f"send: message is too big: {msg}")
            return

        self.transport.sendto(msg.encode(), addr)

    def error_received(self, exc: Exception) -> None:
        self.log.error(f"connection lost: {exc}")

    def connection_lost(self, exc: Exception | None) -> None:
        self.log.debug(f"connection lost: {exc}")

    def parse(self, msg: str) -> tuple[str, str, bool]:
        try:
            hi = msg.index("=")
            insert = True

        except ValueError:
            hi = len(msg)
            insert = False

        return msg[:hi], msg[hi + 1:], insert


async def main() -> None:
    log = logging.getLogger("unusualdb")
    log.info(f"Listening on {address}:{port}")

    loop = asyncio.get_running_loop()
    exit_future: asyncio.Future[bool] = loop.create_future()

    # One protocol instance will be created to serve all client requests.
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UnusualDB(log), local_addr=(address, port))

    try:
        await exit_future

    except KeyboardInterrupt:
        log.info("closing...")

    finally:
        transport.close()
        del protocol


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("interrupt...")
        exit(0)
