import asyncio
import logging
import os
import sys
import time

from typing import Self

# 07. Line Reversal - https://protohackers.com/problem/7

logging.basicConfig(level=int(os.getenv("LOGLEVEL", logging.INFO)),
                    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "127.0.0.1")
port = int(os.getenv("UDP_PORT", "5000"))

Address = tuple[str, int]


class ParseError(Exception):
    pass


class SessionError(Exception):
    pass


class App:
    """application layer"""

    def __init__(self):
        self.chunks: list[bytes] = []

    def read(self) -> bytes:
        data = b"".join(self.chunks)

        lo = 0
        buf: list[bytes] = []
        line: list[int] = []
        for hi, ch in enumerate(data):
            line.append(ch)

            if ch == ord("\n"):
                lo = hi + 1
                buf.append(self.reverse(bytes(line)))
                line = []

        if lo < len(data):
            self.chunks = [data[lo:]]

        return b"".join(buf)

    def write(self, buf: bytes) -> None:
        self.chunks.append(buf)

    def reverse(self, buf: bytes) -> bytes:
        # join reversed substring and the last character(newline)
        return b"".join([buf[-2::-1], bytes([buf[-1]])])


class Message:
    """LRCP message"""

    min_sid = 0
    max_sid = 2147483648

    def __init__(self, data: bytes) -> None:
        self.pos: int | None = None
        self.data: str | None = None
        self.parse(data.decode())

    def parse(self, line: str) -> Self:
        if line[0] != "/" or line[len(line) - 1] != "/":
            raise ParseError(f"invalid message: {line}")

        try:
            self.type, ssid, *tokens = line[1:-1].split("/")
        except ValueError:
            raise ParseError(
                f"invalid message - type and sid are required: {line}")

        try:
            self.sid = int(ssid, 10)
        except ValueError:
            raise ParseError(f"sid is not a number: {ssid}")

        if self.sid < Message.min_sid or self.sid > Message.max_sid:
            raise ParseError(
                f"sid is out of range {Message.min_sid}-{Message.max_sid}")

        if len(tokens) > 0:
            spos, *data = tokens

            try:
                self.pos = int(spos, 10)
            except ValueError:
                raise ParseError(f"pos is not a number: {ssid}")

            if len(data) > 1:
                for tok in data[:-1]:
                    if tok[-1] != "\\":
                        raise ParseError("too many fields")

            self.data = "/".join(data)

        return self

    def __str__(self) -> str:
        if self.data:
            return f"{self.type}::{self.sid} {self.pos} [{self.data}]"
        elif self.pos:
            return f"{self.type}::{self.sid} {self.pos}"
        else:
            return f"{self.type}::{self.sid}"


class Session:
    """LRCP message protocol"""

    session_timeout = 60
    retransmit_interval = 3
    max_payload_size = 800

    def __init__(self, logger: logging.Logger, sid: int,
                 transport: asyncio.DatagramTransport, addr: Address) -> None:
        self.log = logger
        self.sid = sid
        self.transport = transport
        self.addr = addr
        self.closed = False
        self.rcv_acked = 0
        self.rcv_last = time.time()
        self.send_total = 0
        self.send_acked = 0
        self.send_chunks: list[tuple[int, str]] = []
        self.send_rtx = None
        self.send_rtx_closed = asyncio.Event()
        self.app = App()

    def handle(self, msg: Message) -> None:
        if msg.type == "connect":
            self.handle_connect(msg)
        elif msg.type == "close":
            self.handle_close(msg)
        elif msg.type == "ack":
            self.handle_ack(msg)
        elif msg.type == "data":
            self.handle_data(msg)
        else:
            raise SessionError(f"invalid message type: {msg.type}")

    def handle_connect(self, msg: Message) -> None:
        self.log.debug("handling connect message")
        if self.closed:
            self.closed = False
        self.send_ack(0)

    def handle_close(self, msg: Message) -> None:
        self.log.debug("handling close message")
        self.send_close()
        self.close()

    def handle_ack(self, msg: Message) -> None:
        self.log.debug(f"handling ack message: pos:{msg.pos}")
        self.notify()

        if msg.pos is None:
            self.log.error(f"invalid message: {msg}")
            return

        if msg.pos > self.send_total:
            self.send_close()
            self.close()
        else:
            self.send_acked = msg.pos

    def handle_data(self, msg: Message) -> None:
        self.log.debug(f"handling data message: pos:{msg.pos} data:{msg.data}")
        self.notify()

        if msg.pos is None or msg.data is None:
            self.log.error(f"invalid message: {msg}")
            return

        if msg.pos > self.rcv_acked:
            # do not have all the data - send duplicate ack for data we have.
            self.send_ack(self.rcv_acked)
            self.send_ack(self.rcv_acked)

        elif msg.pos < self.rcv_acked and not self.closed:
            self.send_ack(self.rcv_acked)
            # resend all unacked chunks
            self.send_data(msg.pos)
            self.create_retransmit_task()

        else:
            # have all data up to pos + the current buffer
            buf = self.unescape(msg.data)
            self.rcv_acked += len(buf)
            self.send_ack(self.rcv_acked)
            self.process_app_data(buf)

    def process_app_data(self, buf: str) -> None:
        if self.closed:
            return

        self.app.write(buf.encode())
        resp = self.app.read().decode()
        if len(resp) > 0:
            self.send_chunks.append((self.send_total, resp))
            self.send_data(self.send_total)
            self.send_total += len(resp)
            self.create_retransmit_task()

    def notify(self) -> None:
        self.rcv_last = time.time()

    def is_expired(self) -> bool:
        return time.time() - self.rcv_last > Session.session_timeout

    def is_closed(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True
        self.send_rtx_closed.set()
        if self.send_rtx:
            self.send_rtx.cancel()

    def escape(self, data: str) -> str:
        chunks: list[str] = []
        lo = 0
        for hi in range(len(data)):
            if data[hi] == "/":
                chunks.append(data[lo:hi])
                chunks.append("\\/")
                lo = hi + 1

            elif data[hi] == "\\":
                chunks.append(data[lo:hi])
                chunks.append("\\\\")
                lo = hi + 1

        chunks.append(data[lo:])
        return "".join(chunks)

    def unescape(self, data: str) -> str:
        return data.replace("\\/", "/").replace("\\\\", "\\")

    async def retransmit(self) -> None:
        self.log.debug("start retransmit")
        await asyncio.sleep(Session.retransmit_interval)
        while (self.send_acked < self.send_total
               and not self.send_rtx_closed.is_set()):
            self.log.debug(f"retransmitting {self.send_acked}")
            self.send_data(self.send_acked)
            await asyncio.sleep(Session.retransmit_interval)
        if self.send_rtx is not None:
            self.send_rtx.cancel()
            self.send_rtx = None
        self.send_rtx_closed.clear()

    def create_retransmit_task(self) -> None:
        if self.send_rtx is None:
            self.log.debug("start retransmit task")
            self.send_rtx = asyncio.Task(self.retransmit())

    def send_ack(self, pos: int) -> None:
        self.send(f"/ack/{self.sid}/{pos}/")

    def send_data(self, pos: int) -> None:
        for chunk_pos, data in self.send_chunks:
            if pos >= chunk_pos + len(data):
                continue

            if pos > chunk_pos:
                self.send_data_chunk(pos, data[pos - chunk_pos:])
            else:
                self.send_data_chunk(chunk_pos, data)

    def send_data_chunk(self, pos: int, data: str) -> None:
        self.log.debug(f"sending data: {pos} {data}")
        self.send(f"/data/{self.sid}/{pos}/{self.escape(data)}/")

    def send_close(self) -> None:
        self.send(f"/close/{self.sid}/")

    def send(self, msg: str) -> None:
        if len(msg) > 1000:
            self.log.error(f"send: message is too big: {msg}")
            return

        self.log.debug(f"send: {msg}")
        self.transport.sendto(msg.encode(), self.addr)


class LRCP(asyncio.DatagramProtocol):
    sweeper_interval = 5

    def __init__(self, logger: logging.Logger,
                 close_event: asyncio.Event) -> None:
        super().__init__()
        self.log = logger
        self.close_event = close_event
        self.sessions: dict[int, Session] = {}
        self.log.info("initialised")
        self.sweeper_task = asyncio.Task(self.sweep_sessions())

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Address) -> None:
        addr_port = ":".join([str(a) for a in addr])

        try:
            msg = Message(data)
            log = self.log.getChild(f"{addr_port}:sid-{msg.sid}")
            try:
                session = self.sessions[msg.sid]
            except KeyError:
                session = Session(log, msg.sid, self.transport, addr)

                if msg.type != "connect":
                    session.send_close()
                    session.close()
                    return

                self.sessions[msg.sid] = session
            session.handle(msg)

        except ParseError as err:
            self.log.error(f"parse error: {err}")

    def error_received(self, exc: Exception) -> None:
        self.log.error(f"connection lost: {exc}")

    def connection_lost(self, exc: Exception | None) -> None:
        self.log.debug(f"connection lost: {exc}")

    async def sweep_sessions(self) -> None:
        while not self.close_event.is_set():
            for sid, session in self.sessions.items():
                if session.is_closed():
                    del self.sessions[sid]
                    continue

                if session.is_expired():
                    self.log.info(f"session has expired: {sid}")
                    session.close()
                    del self.sessions[sid]

            await asyncio.sleep(LRCP.sweeper_interval)
        self.sweeper_task.cancel()


async def main(close_event: asyncio.Event) -> None:
    log = logging.getLogger("lrcp")
    log.info(f"Listening on {address}:{port}")

    loop = asyncio.get_running_loop()
    exit_future: asyncio.Future[bool] = loop.create_future()

    # One protocol instance will be created to serve all client requests.
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: LRCP(log, close_event), local_addr=(address, port))

    try:
        await exit_future

    except KeyboardInterrupt:
        log.info("closing...")

    finally:
        close_event.set()
        transport.close()
        del protocol


if __name__ == "__main__":
    close_event = asyncio.Event()

    try:
        asyncio.run(main(close_event))

    except KeyboardInterrupt:
        close_event.set()
        print("interrupt...")
        exit(0)
