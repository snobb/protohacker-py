import asyncio
import logging
import os
import re
import sys

# 03. Budget Chat - https://protohackers.com/problem/3

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))

chat: dict[str, asyncio.StreamWriter] = {}


class Client():

    async def handle(self, reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter) -> None:
        writer.write(b"Welcome to budgetchat! What shall I call you?\n")
        await writer.drain()

        name = await reader.readline()
        self.name = name.decode().strip()
        self.validate_name()

        await self.register(writer)

        while not reader.at_eof():
            line = await reader.readline()
            if not line:
                break  # eof

            await self.broadcast(b" ".join([f"[{self.name}]".encode(), line]))

    def validate_name(self) -> None:
        if self.name in chat:
            raise ValueError("duplicate name: {self.name}")

        if len(self.name) == 0 or not re.match("^[a-zA-Z0-9]*$", self.name):
            raise ValueError(f"invalid name: {self.name}")

    async def register(self, writer: asyncio.StreamWriter) -> None:
        names = ", ".join([name for name in chat])
        writer.write(f"* room contains: {names}\n".encode())
        await writer.drain()

        await self.broadcast(f"* {self.name} has entered the room\n".encode())
        chat[self.name] = writer

    async def broadcast(self, msg: bytes) -> None:
        for name, sock in chat.items():
            if self.name != name:
                sock.write(msg)
                await sock.drain()

    async def unregister(self) -> None:
        try:
            del chat[self.name]
            await self.broadcast(f"* {self.name} has left the room\n".encode())

        except KeyError:
            return


async def handler(reader: asyncio.StreamReader,
                  writer: asyncio.StreamWriter) -> None:
    peer = ":".join(str(tok) for tok in writer.get_extra_info("peername"))
    log = logging.getLogger(peer)

    client = Client()
    log.info("connected")

    try:
        await client.handle(reader, writer)

    except Exception as e:
        log.error(f"error: {e}")

    finally:
        log.info("disconnected")
        await client.unregister()
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
