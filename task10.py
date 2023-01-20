import asyncio
import logging
import os
import string
import sys

# 10. Voracious Code Storage - https://protohackers.com/problem/10

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))

store: dict[str, list[bytes]] = {}


class ValidationError(Exception):
    pass


class Client():

    def __init__(self, logger: logging.Logger, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
        self.log = logger
        self.reader = reader
        self.writer = writer

    async def handle(self) -> None:

        while not self.reader.at_eof():
            try:
                self.send("READY")
                await self.writer.drain()

                line = await self.reader.readline()
                self.log.debug(f"line: {line.decode()}")

                args = line.decode().split()
                if len(args) == 0:
                    self.send("ERR illegal method:")
                    return

                cmd = args.pop(0).lower()
                tokens = args

                if cmd == "put":
                    try:
                        file, length = self.validate_put(tokens)
                        await self.put_file(file, length)
                    except ValidationError as e:
                        self.send(f"{e}")
                        continue

                elif cmd == "get":
                    try:
                        file, rev = self.validate_get(tokens)
                        await self.get_file(file, rev)
                    except ValidationError as e:
                        self.send(f"{e}")
                        continue

                elif cmd == "list":
                    try:
                        dir = self.validate_list(tokens)
                        await self.list_dir(dir)
                    except ValidationError as e:
                        self.send(f"{e}")
                        continue

                elif cmd == "help":
                    self.send("OK usage: HELP|GET|PUT|LIST")

                elif cmd == "clean-data":
                    store.clear()

                else:
                    self.send(f"ERR illegal method: {cmd}")
                    return
            finally:
                await self.writer.drain()

    def validate_put(self, tokens: list[str]) -> tuple[str, int]:
        if len(tokens) != 2:
            raise ValidationError("ERR usage: PUT file length newline data")
        name = self.validate_name(tokens[0])
        return (name, int(tokens[1]))

    def validate_get(self, tokens: list[str]) -> tuple[str, int]:
        if len(tokens) < 1 or len(tokens) > 2:
            raise ValidationError("ERR usage: GET file [revision]")
        self.validate_name(tokens[0])

        try:
            rstr = tokens[1]
            if rstr and rstr.startswith("r"):
                rstr = rstr[1:]

            rev = int(rstr)

            if rev < 1:
                raise ValidationError("ERR no such revision")

        except IndexError:  # no rev provided
            rev = -1
        except ValueError:  # rev is not a number.
            raise ValidationError("ERR no such revision")

        return (tokens[0], rev)

    def validate_list(self, tokens: list[str]) -> str:
        if len(tokens) != 1:
            raise ValidationError("ERR usage: LIST dir")
        self.validate_name(tokens[0])
        return tokens[0]

    def validate_name(self, name: str) -> str:
        if len(name) == 0 or name[0] != "/":
            raise ValidationError("ERR illegal file name")

        for ch in name[1:]:
            if not ch.isalnum() and ch not in ["-", "_", ".", "/"]:
                raise ValidationError("ERR illegal file name")

        if name[1:].find("//") > 0:
            raise ValidationError("ERR illegal file name")

        return name

    async def put_file(self, file: str, length: int) -> None:
        data = await self.reader.readexactly(length)

        for ch in data:
            if ch not in bytes(string.printable, "ascii"):
                self.send("ERR text files only")
                return

        lst = store.setdefault(file, [])
        for i, stored in enumerate(lst):
            if stored == data:
                self.send(f"OK r{i+1}")  # existing revision
                return

        lst.append(data)
        self.send(f"OK r{len(lst)}")

    async def get_file(self, file: str, rev: int = -1) -> None:
        if file in store:
            lst = store[file]

            rv = rev if rev == -1 else rev - 1

            if rv >= len(lst):
                self.send("ERR no such revision")
                return

            data = lst[rv]
            self.send(f"OK {len(data)}")
            self.writer.write(data)

        else:
            self.send("ERR no such file")

    async def list_dir(self, dir: str) -> None:
        if dir[-1] != "/":
            dir = f"{dir}/"

        dirs: set[str] = set()
        files: list[tuple[str, int]] = []
        for k, v in store.items():
            if k.startswith(dir):
                value = k[len(dir):]
                idx = value.find("/")
                if idx != -1 and idx < len(value) - 1:
                    dirs.add(value[:idx + 1])
                else:
                    files.append((k[len(dir):], len(v)))

        self.send(f"OK {len(files) + len(dirs)}")
        for dir in sorted(dirs):
            self.send(f"{dir} DIR")

        for file, sz in sorted(files):
            self.send(f"{file} r{sz}")

    def send(self, msg: str) -> None:
        self.log.debug("msg")
        self.writer.write(f"{msg}\n".encode())


async def handler(reader: asyncio.StreamReader,
                  writer: asyncio.StreamWriter) -> None:
    peer = ":".join(str(tok) for tok in writer.get_extra_info("peername"))
    log = logging.getLogger(peer)

    log.info("connected")

    try:
        await Client(log, reader, writer).handle()

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
        print("interrupt...")
        exit(0)
