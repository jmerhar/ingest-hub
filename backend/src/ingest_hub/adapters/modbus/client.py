"""A minimal Modbus TCP client that can only read registers.

It speaks function codes 3 (read holding registers) and 4 (read input registers) and nothing
else, so a source built on it can never change a device's settings. One request is in flight at a
time on one connection, kept open between reads.
"""

from __future__ import annotations

import asyncio
import contextlib
import struct

#: Function codes this client sends: read holding registers and read input registers.
READ_FUNCTIONS = frozenset({3, 4})
#: The most registers one read may ask for, by the Modbus specification.
MAX_REGISTERS = 125
#: Modbus exception 2: the device has no register at an address asked for.
ILLEGAL_ADDRESS = 2


class ModbusError(Exception):
    """The device answered a read with a Modbus exception."""

    def __init__(self, code: int, function: int, address: int, count: int) -> None:
        super().__init__(
            f"Modbus exception {code} reading {count} register(s) at {address} "
            f"(function {function})"
        )
        self.code = code


class ModbusClient:
    """One connection to a Modbus TCP device, opened on the first read and reopened after a
    failure."""

    def __init__(self, host: str, port: int, unit: int, timeout: float) -> None:
        self._host = host
        self._port = port
        self._unit = unit
        self._timeout = timeout
        self._streams: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None
        self._transaction = 0

    async def read(self, function: int, address: int, count: int) -> list[int]:
        """Read ``count`` registers from ``address`` as unsigned 16-bit values.

        A Modbus exception raises :class:`ModbusError` and leaves the connection open; anything
        else (a timeout, a dropped connection, a malformed reply) closes it and raises, so the
        next read starts afresh.
        """
        if function not in READ_FUNCTIONS:
            raise ValueError(f"function {function} is not a read")
        if not 1 <= count <= MAX_REGISTERS:
            raise ValueError(f"cannot read {count} registers at once")
        try:
            async with asyncio.timeout(self._timeout):
                return await self._exchange(function, address, count)
        except ModbusError:
            raise
        except BaseException:
            await self.close()
            raise

    async def _exchange(self, function: int, address: int, count: int) -> list[int]:
        reader, writer = await self._connection()
        self._transaction = (self._transaction + 1) % 0x10000
        tid = self._transaction
        writer.write(struct.pack(">HHHBBHH", tid, 0, 6, self._unit, function, address, count))
        await writer.drain()
        head = await reader.readexactly(7)
        got_tid, protocol, length, _unit = struct.unpack(">HHHB", head)
        body = await reader.readexactly(max(0, length - 1))
        if got_tid != tid or protocol != 0 or not body:
            raise ConnectionError("malformed Modbus reply")
        if body[0] == function | 0x80:
            raise ModbusError(body[1] if len(body) > 1 else 0, function, address, count)
        if body[0] != function or len(body) < 2 or body[1] != 2 * count:
            raise ConnectionError("unexpected Modbus reply")
        return list(struct.unpack(f">{count}H", body[2 : 2 + 2 * count]))

    async def _connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._streams is None:
            self._streams = await asyncio.open_connection(self._host, self._port)
        return self._streams

    async def close(self) -> None:
        """Close the connection, if open."""
        if self._streams is not None:
            _, writer = self._streams
            self._streams = None
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
