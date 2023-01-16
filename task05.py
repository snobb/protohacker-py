import asyncio
import logging
import os
import re
import sys

# 05. Mob in the Middle - https://protohackers.com/problem/5

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))

# Proxy backend address and port
be_address = os.getenv("BE_ADDRESS", "127.0.0.1")
be_port = int(os.getenv("BE_PORT", "8888"))


class BogusCoin:
    # EvilAddr where to send stolen monies ;)
    evil_addr = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
    # regexp with lookbehind checking for start of the line or space
    evil_re = re.compile(r"7[0-9a-zA-z]{25,}+\n?")

    def __init__(self, logger: logging.Logger):
        self.log = logger

    async def handle(self, reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter,
                     close_event: asyncio.Event) -> None:
        while not close_event.is_set():
            try:
                buf = await asyncio.wait_for(reader.readline(), 1)
            except asyncio.TimeoutError:
                continue

            if not buf or buf[-1] != ord("\n"):
                close_event.set()
                return  # eof

            line = buf.rstrip().decode()
            self.log.debug(f"line: {line}")

            changed = self.replace_address(line)
            writer.write(f"{changed}\n".encode())
            await writer.drain()

    def replace_address(self, msg: str) -> str:
        chunks: list[str] = []
        lo = 0

        for match in BogusCoin.evil_re.finditer(msg):
            start, end = match.start(), match.end()

            if ((start > lo and msg[start - 1] != " ")
                    or (len(msg) > end and msg[end] != " ")):
                continue

            if 26 < end - start > 35:
                continue

            chunks.append(msg[lo:start])
            chunks.append(BogusCoin.evil_addr)
            lo = end

        chunks.append(msg[lo:])

        return "".join(chunks)


async def handler(fe_read: asyncio.StreamReader,
                  fe_write: asyncio.StreamWriter) -> None:
    peer = ":".join(str(tok) for tok in fe_write.get_extra_info("peername"))
    log = logging.getLogger(peer)

    bogus = BogusCoin(log)
    log.info("connected")

    log.info(f"connect to the backend: {be_address}:{be_port}")
    be_read, be_write = await asyncio.open_connection(be_address, be_port)

    close_event = asyncio.Event()

    try:
        # handle both inbound and outbound sides of the full proxy.
        await asyncio.gather(bogus.handle(fe_read, be_write, close_event),
                             bogus.handle(be_read, fe_write, close_event))

    except Exception as e:
        log.error(f"error: {e}")

    finally:
        close_event.set()

        log.info("disconnected")
        fe_write.close()
        be_write.close()
        await asyncio.gather(fe_write.wait_closed(), be_write.wait_closed())


async def main() -> None:
    server = await asyncio.start_server(handler, address, port)
    addr = ", ".join(str(sock.getsockname()) for sock in server.sockets)

    print(f"Listening on {addr}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("interrupt...")
        exit(0)
