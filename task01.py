import asyncio
import os
import sys
import json
import math
import logging

# 01. Prime Time - https://protohackers.com/problem/1

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    stream=sys.stdout)

address = os.getenv("SOCKET_ADDRESS", "0.0.0.0")
port = int(os.getenv("TCP_PORT", "8080"))


class InvalidRequestError(ValueError):
    pass


def parse_message(line: bytes) -> int:
    try:
        req = json.loads(line.decode())
    except json.JSONDecodeError:
        raise InvalidRequestError(f"invalid json: {line.decode()}")

    try:
        method = req["method"]
        num = req["number"]
    except KeyError as e:
        raise InvalidRequestError(f"Missing field: {e}")

    if method != "isPrime":
        raise InvalidRequestError(f"invalid method: {method}")

    if type(num) != int and type(num) != float:
        raise InvalidRequestError("number field must be a number")

    return int(num)


def get_response(is_prime: bool) -> bytes:
    res = {"method": "isPrime", "prime": is_prime}
    return f"{json.dumps(res)}\n".encode()


def get_error(err: str) -> bytes:
    res = {"method": "error", "error": err}
    return f"{json.dumps(res)}\n".encode()


def is_prime(num: int) -> bool:
    if num <= 1:
        return False

    sqrt = math.sqrt(num)
    for x in range(2, int(sqrt) + 1):
        if num % x == 0:
            return False

    return True


async def prime(reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter) -> None:
    peer = ":".join(str(tok) for tok in writer.get_extra_info("peername"))
    log = logging.getLogger(peer)

    log.info("connected")

    while not reader.at_eof():
        line = await reader.readline()

        if not line:
            break

        log.debug(f"request: {line.decode()}")

        try:
            num = parse_message(line)
            res = get_response(is_prime(num))
            log.debug(f"response: {res.decode()}")
            writer.write(res)

        except InvalidRequestError as e:
            log.error(f"error: {line.decode()}")
            writer.write(get_error(str(e)))
            break

        finally:
            await writer.drain()

    log.info("disconnected")
    writer.close()


async def main() -> None:
    server = await asyncio.start_server(prime, address, port)

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
