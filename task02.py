import asyncio
import logging
import os
import struct
import sys

# 02. Means to an End - https://protohackers.com/problem/2

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))

TYPE_INSERT = b'I'
TYPE_QUERY = b'Q'

PriceRecord = tuple[int, int]


class Price():

    def __init__(self, peer: str, logger: logging.Logger) -> None:
        self.price_records: list[PriceRecord] = []
        self.log = logger
        self.need = 9

    async def handle(self, reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter) -> None:
        chunks: list[bytes] = []

        while not reader.at_eof():
            buf = await reader.read(self.need)
            if not buf:
                break  # eof

            chunks.append(buf)

            if len(buf) < self.need:
                self.need -= len(buf)
                continue

            msg_type, time, data = struct.unpack(">cii", b"".join(chunks))
            self.log.debug(f"message: [{msg_type}, {time}, {data}]")

            self.need, chunks = 9, []

            if msg_type == TYPE_INSERT:
                self.price_records.append((time, data))

            elif msg_type == TYPE_QUERY:
                prices = [
                    pr for (tm, pr) in self.price_records
                    if tm >= time and tm <= data
                ]
                mean = self.mean(prices)
                self.log.debug(f"mean: {mean}")
                writer.write(struct.pack(">i", self.mean(prices)))
                await writer.drain()

            else:
                raise ValueError(f"Invalid message type: {msg_type.decode()}")

    def mean(self, data: list[int]) -> int:
        if len(data) == 0:
            return 0

        return int(sum(data) / len(data))


async def handler(reader: asyncio.StreamReader,
                  writer: asyncio.StreamWriter) -> None:
    peer = ":".join(str(tok) for tok in writer.get_extra_info("peername"))
    log = logging.getLogger(peer)

    log.info("connected")
    try:
        await Price(peer, log).handle(reader, writer)

    except Exception as e:
        log.error(f"error: {e}")

    finally:
        log.info("disconnected")
        writer.close()


async def main() -> None:
    server = await asyncio.start_server(handler, address, port)

    addr = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addr}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('interrupt...')
        exit(0)
