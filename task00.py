import asyncio
import os

# 00. Smoke Test - https://protohackers.com/problem/0

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))
bufsize = 100


async def handle_echo(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    print(f"connected from {peer}")

    while not reader.at_eof():
        data = await reader.read(bufsize)

        writer.write(data)
        await writer.drain()

    print(f'disconnected from {peer}')
    writer.close()


async def main() -> None:
    server = await asyncio.start_server(handle_echo, address, port)

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
