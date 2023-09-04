import asyncio
import logging
import os
import struct
import sys
import math

from typing import Self
from enum import IntEnum

# 06. Speed Daemon - https://protohackers.com/problem/6

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))

Reading = tuple[int, int]
issued_tickets: dict[int, list['Ticket']] = {}
plate_readings: dict[str, list[Reading]] = {}
ticket_days: dict[bytes, set[int]] = {}
dispatchers: dict[int, asyncio.StreamWriter] = {}


class MsgType(IntEnum):
    ERROR = 0x10
    PLATE = 0x20
    TICKET = 0x21
    WANT_HEARTBEAT = 0x40
    HEARTBEAT = 0x41
    IAM_CAMERA = 0x80
    IAM_DISPATCHER = 0x81


class Plate:

    def __init__(self, plate: bytes = b"", timestamp: int = 0):
        self.plate = plate
        self.timestamp = timestamp

    async def read_from(self, reader: asyncio.StreamReader) -> Self:
        strlen = (await reader.readexactly(1))[0]
        self.plate = await reader.readexactly(strlen)
        buf = await reader.readexactly(4)
        self.timestamp = struct.unpack("!I", buf)[0]
        return self

    def __str__(self) -> str:
        return f"plate:{self.plate.decode()} timestamp:{self.timestamp}"


class Camera:

    def __init__(self, road: int = 0, mile: int = 0, limit: int = 0):
        self.road = road
        self.mile = mile
        self.limit = limit

    async def read_from(self, reader: asyncio.StreamReader) -> Self:
        buf = await reader.readexactly(6)
        self.road, self.mile, self.limit = struct.unpack("!HHH", buf)
        return self

    def __str__(self) -> str:
        return f"road:{self.road} mile:{self.mile} limit:{self.limit}"


class Dispatcher:

    def __init__(self, roads: list[int] = []):
        self.roads = roads

    async def read_from(self, reader: asyncio.StreamReader) -> Self:
        rlen = (await reader.readexactly(1))[0]
        buf = await reader.readexactly(rlen * 2)
        self.roads = list(struct.unpack(f"!{'H'*rlen}", buf))
        return self

    def __str__(self) -> str:
        return f"roads:{self.roads}"


class Ticket:

    def __init__(self, plate: bytes, road: int, mile1: int, timestamp1: int,
                 mile2: int, timestamp2: int, speed: int) -> None:
        self.plate = plate
        self.road = road
        self.mile1 = mile1
        self.timestamp1 = timestamp1
        self.mile2 = mile2
        self.timestamp2 = timestamp2
        self.speed = speed

    async def write_to(self, writer: asyncio.StreamWriter) -> None:
        print(f"Sending ticket {self}")
        writer.write(
            struct.pack(f"!BB{len(self.plate)}sHHIHIH", MsgType.TICKET,
                        len(self.plate), self.plate, self.road, self.mile1,
                        self.timestamp1, self.mile2, self.timestamp2,
                        self.speed))
        await writer.drain()

    def __str__(self) -> str:
        return (f"plate:{self.plate.decode()} road:{self.road} "
                f"mile1:{self.mile1} timestamp1:{self.timestamp1} "
                f"mile2:{self.mile2} timestamp:{self.timestamp2} "
                f"speed:{self.speed}")


class SpeedError(Exception):

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

    async def write_to(self, writer: asyncio.StreamWriter) -> None:
        msg = self.message.encode()
        writer.write(
            struct.pack(f"!BB{len(msg)}s", MsgType.ERROR, len(msg), msg))
        await writer.drain()

    def __str__(self) -> str:
        return f"error: {self.message}"


class Session:

    def __init__(self, logger: logging.Logger, close_event: asyncio.Event,
                 reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.log = logger
        self.close_event = close_event
        self.reader = reader
        self.writer = writer
        self.camera: Camera | None = None
        self.dispatcher: Dispatcher | None = None
        self.heartbeat_task: asyncio.Task[None] | None = None

    def is_running(self) -> bool:
        return not self.close_event.is_set()

    async def handle(self) -> None:
        while self.is_running():
            try:
                type = await self.reader.readexactly(1)

                if type[0] == MsgType.WANT_HEARTBEAT:
                    buf = await self.reader.readexactly(4)
                    interval = int(struct.unpack("!I", buf)[0])
                    if interval > 0:
                        self.heartbeat_task = asyncio.Task(
                            self.heartbeat(interval))

                elif type[0] == MsgType.PLATE:
                    plate = await Plate().read_from(self.reader)
                    self.log.debug(f"handling plate: {plate}")
                    await self.handle_plate(plate)

                elif type[0] == MsgType.IAM_CAMERA:
                    if self.camera:
                        raise SpeedError("duplicate camera")

                    camera = await Camera().read_from(self.reader)
                    self.log.debug(f"handling camera: {camera}")
                    await self.handle_camera(camera)

                elif type[0] == MsgType.IAM_DISPATCHER:
                    if self.camera:
                        raise SpeedError("duplicate camera")

                    if self.dispatcher:
                        raise SpeedError("duplicate dispatcher")

                    dispatcher = await Dispatcher().read_from(self.reader)
                    self.log.debug(f"handling dispatcher: {dispatcher}")
                    await self.handle_dispatcher(dispatcher)

                else:
                    raise SpeedError("invalid message type")

            except SpeedError as err:
                self.log.error(err)
                await err.write_to(self.writer)

            except asyncio.IncompleteReadError as err:
                self.log.debug(f"unexpected eof: {err}")
                return

            except Exception as err:
                self.log.error(f"error: {err}")
                return

    async def heartbeat(self, interval: int) -> None:
        while self.is_running():
            self.writer.write(MsgType.HEARTBEAT.to_bytes())
            await self.writer.drain()
            self.log.debug("heartbeat")
            await asyncio.sleep(interval / 10)

    async def read_want_heartbeat(self) -> int:
        buf = await self.reader.readexactly(4)
        return int(struct.unpack("!I", buf)[0])

    async def handle_plate(self, plate: Plate) -> None:
        if not self.camera:
            raise SpeedError(
                'No cameras has been registered yet for this road')

        self.register_plate(plate.plate, plate.timestamp)
        self.issue_tickets(plate.plate)
        await self.send_tickets(self.camera.road)

    async def handle_camera(self, camera: Camera) -> None:
        self.camera = camera
        self.log.info(f"registering camera at road {camera.road} "
                      f"[mile: {camera.mile}, limit: {camera.limit}]")

    async def handle_dispatcher(self, dispatcher: Dispatcher) -> None:
        self.log.info(f"new dispatcher: {dispatcher}")
        self.dispatcher = dispatcher

        for road in self.dispatcher.roads:
            self.register_dispatcher(road, self.writer)
            await self.send_tickets(road)

    def gen_key(self, plate: bytes) -> str:
        assert (self.camera is not None)
        return f"{plate.decode()}::{self.camera.road}"

    def register_plate(self, plate: bytes, timestamp: int) -> None:
        assert (self.camera is not None)
        key = self.gen_key(plate)
        road, mile = self.camera.road, self.camera.mile
        plate_readings.setdefault(key, []).append((mile, timestamp))

        self.log.info(f"Registering plate {plate.decode()} for road {road}: "
                      f"[mile: {mile}, time: {timestamp}]")

    def issue_tickets(self, plate: bytes) -> None:
        if not self.camera:
            raise SpeedError("camera is not set")

        key = self.gen_key(plate)

        records = plate_readings.setdefault(key, [])
        if len(records) > 1:
            records.sort(key=lambda a: a[1])

        for i, j in enumerate(range(1, len(records))):
            (mile1, ts1), (mile2, ts2) = records[i], records[j]
            distance = abs(mile2 - mile1)
            time = ts2 - ts1
            if time == 0:
                continue
            speed = distance / time * 3600
            self.log.info(f"calculated speed: {math.floor(speed)}, "
                          f"limit: {self.camera.limit}")

            if speed > (self.camera.limit + 0.3):
                self.log.info(f"speeding detected for {plate.decode()} - "
                              f"speed: {math.floor(speed)}, "
                              f"limit: {self.camera.limit}")

                self.track_ticket(
                    Ticket(plate, self.camera.road, mile1, ts1, mile2, ts2,
                           math.floor(speed * 100)))

    def track_ticket(self, ticket: Ticket) -> None:
        day1 = ticket.timestamp1 // 86400
        day2 = ticket.timestamp2 // 86400
        for i in range(day1, day2 + 1):
            if i in ticket_days.setdefault(ticket.plate, set()):
                self.log.info(
                    f"{ticket.plate.decode()} has already been ticketed "
                    f"on the {i} day")
                return

        for i in range(day1, day2 + 1):
            ticket_days[ticket.plate].add(i)

        # add ticket
        issued_tickets.setdefault(ticket.road, []).append(ticket)

    async def send_tickets(self, road: int) -> None:
        if road not in dispatchers:
            self.log.info("No dispatchers for the road {road} yet")
            return

        writer = dispatchers[road]
        tickets = issued_tickets.get(road, [])

        while len(tickets) > 0:
            ticket = tickets.pop(0)
            await ticket.write_to(writer)

    def register_dispatcher(self, road: int,
                            writer: asyncio.StreamWriter) -> None:
        dispatchers[road] = writer


async def handler(reader: asyncio.StreamReader,
                  writer: asyncio.StreamWriter) -> None:
    peer = ":".join(str(tok) for tok in writer.get_extra_info("peername"))
    log = logging.getLogger(peer)

    close_event = asyncio.Event()
    session = Session(log, close_event, reader, writer)
    log.info("connected")

    try:
        await session.handle()

    except Exception as e:
        log.error(f"error: {e}")

    finally:
        close_event.set()

        log.info("disconnected")
        writer.close()


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
