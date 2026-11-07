"""Register maps: which registers a device has, how to decode them, and which table each goes to.

A map is a TOML file::

    description = "..."
    table_prefix = "alphaess_"
    function = 3          # 3: holding registers, 4: input registers
    max_read = 100        # registers per request
    max_gap = 8           # unused registers a request may span to join two neighbours

    [tables.inverter_info]
    every_seconds = 3600  # read less often than the source's interval

    [[registers]]         # one value at a register address
    table = "battery"
    name = "soc"
    address = 0x0102
    type = "u16"          # u16, s16, u32, s32 (big-endian words), str (ASCII), hex (hex digits)
    length = 1            # registers, for str and hex
    scale = 0.1
    offset = 0            # value = raw * scale - offset
    unit = "%"

    [[repeats]]           # the same block for each of several slots
    table = "port"
    start = 0x1000        # the first slot's first register
    stride = 40           # registers from one slot to the next
    length = 20           # registers read per slot
    max_count = 100
    stop_when_zero = ["serial"]   # the first slot where this is zero ends the list

    [[repeats.fields]]    # a value at a byte offset within a slot
    name = "serial"
    byte = 1
    type = "hex"          # u8, u16, s16, u32, s32, hex (bytes as hex digits), str
    length = 6            # bytes, for hex and str
    tag = true            # a tag on the slot's point rather than a field

A number is an integer when its scale is 1 and its offset 0, else a float, so a field's type is
fixed by the map and never changes between readings.
"""

from __future__ import annotations

import struct
import tomllib
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Literal

from jigkit.sinks import FieldValue
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ingest_hub.adapters.modbus.client import MAX_REGISTERS

NAME = r"^[a-z_][a-z0-9_]*$"
WORD_TYPES = {"u16": (">H", 1), "s16": (">h", 1), "u32": (">I", 2), "s32": (">i", 2)}
BYTE_TYPES = {"u8": ">B", "u16": ">H", "s16": ">h", "u32": ">I", "s32": ">i"}


class MapError(Exception):
    """A register map cannot be read or makes no sense; the message says why."""


class _Value(BaseModel):
    """What registers and fields share: a name, a scale and an offset."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME)
    scale: float = 1.0
    offset: float = 0.0
    unit: str = ""

    def number(self, raw: int) -> FieldValue:
        """A decoded integer, scaled; an integer when the map does not scale it."""
        if self.scale == 1 and self.offset == 0:
            return raw
        # Rounded, so 393 at 0.1 is 39.3 rather than 39.300000000000004.
        return round(raw * self.scale - self.offset, 9)


class Register(_Value):
    """One value at a register address."""

    table: str = Field(pattern=NAME)
    address: int = Field(ge=0, le=0xFFFF)
    type: Literal["u16", "s16", "u32", "s32", "str", "hex"] = "u16"
    length: int = Field(1, ge=1, le=MAX_REGISTERS)
    function: Literal[3, 4] | None = None

    @property
    def size(self) -> int:
        """Registers the value occupies."""
        return self.length if self.type in ("str", "hex") else WORD_TYPES[self.type][1]

    def decode(self, words: list[int]) -> FieldValue | None:
        """The value from its registers, or None for an empty string."""
        data = struct.pack(f">{len(words)}H", *words)
        if self.type == "str":
            return _text(data)
        if self.type == "hex":
            return data.hex()
        fmt, _ = WORD_TYPES[self.type]
        return self.number(struct.unpack(fmt, data)[0])


class RepeatField(_Value):
    """One value at a byte offset within each slot of a repeated block."""

    byte: int = Field(ge=0)
    type: Literal["u8", "u16", "s16", "u32", "s32", "hex", "str"] = "u16"
    length: int = Field(1, ge=1)
    tag: bool = False

    @property
    def size(self) -> int:
        """Bytes the value occupies."""
        if self.type in ("hex", "str"):
            return self.length
        return struct.calcsize(BYTE_TYPES[self.type])

    def decode(self, data: bytes) -> FieldValue | None:
        """The value from a slot's bytes, or None for an empty string."""
        chunk = data[self.byte : self.byte + self.size]
        if self.type == "hex":
            return chunk.hex()
        if self.type == "str":
            return _text(chunk)
        return self.number(struct.unpack(BYTE_TYPES[self.type], chunk)[0])

    def is_zero(self, data: bytes) -> bool:
        """Whether the field's bytes are all zero."""
        return not any(data[self.byte : self.byte + self.size])


class Repeat(BaseModel):
    """The same block of registers for each of several slots, such as one per inverter port."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(pattern=NAME)
    start: int = Field(ge=0, le=0xFFFF)
    stride: int = Field(ge=1)
    length: int = Field(ge=1, le=MAX_REGISTERS)
    max_count: int = Field(1, ge=1)
    function: Literal[3, 4] | None = None
    stop_when_zero: list[str] = []
    fields: list[RepeatField]

    @model_validator(mode="after")
    def _check(self) -> Repeat:
        names = {f.name for f in self.fields}
        for name in self.stop_when_zero:
            if name not in names:
                raise ValueError(f"stop_when_zero names {name!r}, which is not a field")
        for f in self.fields:
            if f.byte + f.size > 2 * self.length:
                raise ValueError(f"field {f.name!r} ends beyond the {self.length} registers read")
        return self


class TableOptions(BaseModel):
    """How often a table is read."""

    model_config = ConfigDict(extra="forbid")

    every_seconds: float | None = Field(None, gt=0)


class RegisterMap(BaseModel):
    """A device's registers."""

    model_config = ConfigDict(extra="forbid")

    description: str = ""
    table_prefix: str = Field("", pattern=r"^([A-Za-z_][A-Za-z0-9_]*)?$")
    function: Literal[3, 4] = 3
    max_read: int = Field(100, ge=1, le=MAX_REGISTERS)
    max_gap: int = Field(8, ge=0)
    tables: dict[str, TableOptions] = {}
    registers: list[Register] = []
    repeats: list[Repeat] = []

    @model_validator(mode="after")
    def _check(self) -> RegisterMap:
        seen: set[tuple[str, str]] = set()
        for r in self.registers:
            if (r.table, r.name) in seen:
                raise ValueError(f"{r.table}.{r.name} is defined twice")
            seen.add((r.table, r.name))
            if r.size > self.max_read:
                raise ValueError(f"{r.table}.{r.name} is longer than max_read")
        return self

    def every(self, table: str) -> float | None:
        """How often a table is read, when it is read less often than every poll."""
        options = self.tables.get(table)
        return options.every_seconds if options else None


@dataclass(frozen=True)
class Read:
    """One request: a span of registers and the values in it."""

    function: int
    address: int
    count: int
    registers: tuple[Register, ...]


def plan(
    registers: list[Register], default_function: int, max_read: int, max_gap: int
) -> list[Read]:
    """Group registers into as few requests as possible: neighbours with at most ``max_gap``
    unused registers between them share a request, up to ``max_read`` registers each."""
    reads: list[Read] = []
    by_function: dict[int, list[Register]] = {}
    for r in registers:
        by_function.setdefault(r.function or default_function, []).append(r)
    for function, group in sorted(by_function.items()):
        group.sort(key=lambda r: r.address)
        current: list[Register] = []
        start = end = 0
        for r in group:
            fits = current and r.address - end <= max_gap and r.address + r.size - start <= max_read
            if current and not fits:
                reads.append(Read(function, start, end - start, tuple(current)))
                current = []
            if not current:
                start = r.address
                end = r.address
            current.append(r)
            end = max(end, r.address + r.size)
        # Every group holds at least one register, so a last read is always open here.
        reads.append(Read(function, start, end - start, tuple(current)))
    return reads


def load_map(name: str) -> RegisterMap:
    """A bundled map by name (``alphaess``), or a map file by path."""
    try:
        if name.endswith(".toml") or "/" in name:
            text = Path(name).read_text(encoding="utf-8")
        else:
            text = (
                resources.files("ingest_hub.adapters.modbus")
                .joinpath("maps", f"{name}.toml")
                .read_text(encoding="utf-8")
            )
    except (OSError, FileNotFoundError) as error:
        raise MapError(f"no register map {name!r}: {error}") from error
    try:
        return RegisterMap.model_validate(tomllib.loads(text))
    except (tomllib.TOMLDecodeError, ValueError) as error:
        raise MapError(f"register map {name!r}: {error}") from error


def bundled_maps() -> list[str]:
    """The names of the maps that come with the adapter."""
    folder = resources.files("ingest_hub.adapters.modbus").joinpath("maps")
    return sorted(
        p.name.removesuffix(".toml") for p in folder.iterdir() if p.name.endswith(".toml")
    )


def _text(data: bytes) -> str | None:
    """ASCII text with its padding removed, or None when nothing is left."""
    text = data.replace(b"\x00", b"").decode("ascii", errors="replace").strip()
    return text or None
